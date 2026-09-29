// API GATEWAY (HTTP -> SUBNX)
//
// Traduce peticiones HTTP a eventos SUBNX. Send() bloquea hasta que un worker
// contesta o vence el timeout, y el callback corre en la MISMA goroutine, así
// que es seguro escribir en el http.ResponseWriter dentro de él.
//
// Contrato de errores del SDK:
//
//   - Reply con Status=true  -> se ejecuta el callback, Send devuelve nil
//
//   - Reply con Status=false -> NO se ejecuta el callback, Send devuelve error
//
//   - Sin respuesta a tiempo -> Send devuelve error de timeout
//
//     go run ./cmd/gateway
//
//     curl -X POST localhost:8080/login -d '{"username":"manuel","password":"1234"}'
//     curl -X POST localhost:8080/login -d '{"username":"","password":""}'      # Status=false
//     curl -X POST localhost:8080/telemetry -d '{"cpu":"45%","ram":"2GB"}'      # fire-and-forget
//     curl 'localhost:8080/reporte?seconds=3&timeout=1'                          # timeout
//     curl localhost:8080/fallo                                                  # Status=false
package main

import (
	"context"
	"encoding/json"
	"flag"
	"log"
	"net/http"
	"strconv"
	"strings"
	"time"

	SUBNX "github.com/HManuelCC/subnx-go"
)

func main() {
	host := flag.String("host", "localhost", "host del servidor SUBNX")
	port := flag.String("port", "9000", "puerto del servidor SUBNX")
	name := flag.String("name", "NODO_API", "nombre de este cliente")
	key := flag.String("key", "API_KEY", "api key del cliente")
	useTLS := flag.Bool("tls", false, "conectar con TLS")
	httpAddr := flag.String("http", ":8080", "dirección del servidor HTTP")
	flag.Parse()

	// El gateway solo emite eventos, no registra ninguno con AddEvent.
	client := SUBNX.NewClient(*host, *port, *name, *key, *useTLS)

	// No hay OnConnect: damos un margen para que termine el handshake.
	time.Sleep(2 * time.Second)

	mux := http.NewServeMux()
	mux.HandleFunc("/login", loginHandler(client))
	mux.HandleFunc("/telemetry", telemetryHandler(client))
	mux.HandleFunc("/reporte", reporteHandler(client))
	mux.HandleFunc("/fallo", falloHandler(client))

	srv := &http.Server{Addr: *httpAddr, Handler: mux}
	go func() {
		log.Printf("[API] escuchando en %s", *httpAddr)
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("[API] error HTTP: %v", err)
		}
	}()

	// Bloquea hasta Ctrl+C; luego cerramos HTTP con gracia.
	client.Listen()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_ = srv.Shutdown(ctx)
	log.Println("[API] apagado limpio")
}

// ---------------------------------------------------------------------------
// Handlers
// ---------------------------------------------------------------------------

func loginHandler(client *SUBNX.Client) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "usa POST", http.StatusMethodNotAllowed)
			return
		}
		// Credenciales en el body, nunca en la query string.
		var body map[string]string
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			http.Error(w, "JSON inválido", http.StatusBadRequest)
			return
		}
		forward(w, client, &SUBNX.Event{Event: "login", Data: body}, nil)
	}
}

func telemetryHandler(client *SUBNX.Client) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var body map[string]string
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			http.Error(w, "JSON inválido", http.StatusBadRequest)
			return
		}
		// Sin callback => fire-and-forget: Send regresa en cuanto escribe el paquete.
		if err := client.Send(&SUBNX.Event{Event: "telemetria", Data: body}, nil); err != nil {
			writeJSON(w, http.StatusBadGateway, map[string]any{"status": false, "error": err.Error()})
			return
		}
		writeJSON(w, http.StatusAccepted, map[string]any{"status": true, "message": "encolado"})
	}
}

func reporteHandler(client *SUBNX.Client) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		seconds, _ := strconv.Atoi(r.URL.Query().Get("seconds"))
		if seconds <= 0 {
			seconds = 1
		}

		// Timeout personalizado: *time.Duration (nil = default de 65s del SDK).
		var timeout *time.Duration
		if t, err := strconv.Atoi(r.URL.Query().Get("timeout")); err == nil && t > 0 {
			d := time.Duration(t) * time.Second
			timeout = &d
		}

		forward(w, client, &SUBNX.Event{
			Event: "reporte_lento",
			Data:  map[string]int{"seconds": seconds},
		}, timeout)
	}
}

func falloHandler(client *SUBNX.Client) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		forward(w, client, &SUBNX.Event{Event: "fallo", Data: map[string]string{}}, nil)
	}
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

// forward envía el evento y devuelve al navegador lo que contestó el worker.
func forward(w http.ResponseWriter, client *SUBNX.Client, ev *SUBNX.Event, timeout *time.Duration) {
	err := client.Send(ev, timeout, func(resp SUBNX.State) {
		// Solo se llega aquí con Status=true.
		writeJSON(w, http.StatusOK, resp)
	})
	if err != nil {
		writeJSON(w, statusFromError(err), map[string]any{
			"status": false,
			"error":  err.Error(),
		})
	}
}

// El SDK devuelve errores con fmt.Errorf (sin tipos exportados), así que
// distinguimos por texto. Ver README: conviene exportar ErrTimeout / ServerError.
func statusFromError(err error) int {
	msg := err.Error()
	switch {
	case strings.Contains(msg, "timeout"):
		return http.StatusGatewayTimeout // 504
	case strings.Contains(msg, "error del servidor"):
		return http.StatusBadRequest // 400: el worker respondió Status=false
	default:
		return http.StatusBadGateway // 502: no se pudo escribir al servidor, etc.
	}
}

func writeJSON(w http.ResponseWriter, code int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(v)
}
