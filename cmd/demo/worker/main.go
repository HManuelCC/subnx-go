// WORKER (nodo del grid, sin HTTP)
//
// Regla clave del SDK:
//  1. Primero se registran los eventos con GlobalEvents.AddEvent(...)
//  2. Después se crea el cliente con NewClient(...)
//  3. Listen() bloquea el hilo principal y apaga limpio con Ctrl+C
//
// Levanta varios workers con distinto -name: todos los que registran el mismo
// evento ("login") forman un grid y el balanceador decide a cuál mandárselo.
//
//	go run ./cmd/worker -name worker-1
//	go run ./cmd/worker -name worker-2
//	go run ./cmd/worker -name worker-3
package main

import (
	"flag"
	"log"
	"strings"
	"sync/atomic"
	"time"

	SUBNX "github.com/HManuelCC/subnx-go"
)

func main() {
	host := flag.String("host", "localhost", "host del servidor SUBNX")
	port := flag.String("port", "9000", "puerto del servidor SUBNX")
	name := flag.String("name", "worker-1", "nombre único de este cliente en el grid")
	key := flag.String("key", "API_KEY", "api key del cliente")
	useTLS := flag.Bool("tls", false, "conectar con TLS")
	flag.Parse()

	// 1) Eventos ANTES de la conexión.
	registerEvents(*name)

	// 2) Conexión. Solo devuelve el cliente; el handshake ocurre en segundo plano
	//    y los errores se imprimen en consola (no hay callback OnConnect todavía).
	client := SUBNX.NewClient(*host, *port, *name, *key, *useTLS)
	log.Printf("[%s] conectando a %s:%s (tls=%v)...", *name, *host, *port, *useTLS)

	// 3) Bloquea (0% CPU) hasta Ctrl+C.
	client.Listen()
	log.Printf("[%s] apagado limpio", *name)
}

func registerEvents(workerName string) {
	// Contador thread-safe: el SDK puede ejecutar handlers en paralelo.
	var handled atomic.Int64

	// ------------------------------------------------------------------
	// EVENTO CON RESPUESTA: "login"
	// ------------------------------------------------------------------
	// No hay contrato: Bind rellena cualquier tipo cuyos atributos JSON
	// coincidan con lo que mandó el cliente. Ni el nombre del struct ni el
	// de los campos Go importan, solo los tags json.
	SUBNX.GlobalEvents.AddEvent("login", func(event *SUBNX.Event, conn *SUBNX.Client) {
		var creds struct {
			Username string `json:"username"`
			Password string `json:"password"`
		}
		if err := event.Bind(&creds); err != nil {
			log.Printf("[%s] payload inválido: %v", workerName, err)
			event.Reply(conn, SUBNX.State{
				Status:  false,
				Message: "Error al parsear datos de login",
				Error:   "INVALID_PAYLOAD",
			})
			return
		}

		n := handled.Add(1)
		log.Printf("[%s] login #%d user=%q ServerUID=%s", workerName, n, creds.Username, event.ServerUID)

		// Validación de negocio: Status=false => en el cliente Send devuelve error
		// y el callback NO se ejecuta.
		if strings.TrimSpace(creds.Username) == "" || creds.Password == "" {
			event.Reply(conn, SUBNX.State{
				Status:  false,
				Message: "Credenciales incompletas",
				Error:   "INVALID_CREDENTIALS",
			})
			return
		}

		time.Sleep(150 * time.Millisecond) // simula consulta a BD

		// Data es interface{}: la librería la serializa. Incluimos quién atendió
		// para poder ver el balanceo desde el cliente.
		event.Reply(conn, SUBNX.State{
			Status:  true,
			Message: "Login validado",
			Data: map[string]any{
				"username":   creds.Username,
				"token":      "tok_" + creds.Username + "_" + time.Now().Format("150405"),
				"handled_by": workerName,
			},
		})
	})

	// ------------------------------------------------------------------
	// FIRE-AND-FORGET: "telemetria"
	// ------------------------------------------------------------------
	// El emisor no pasó callback, así que aquí no es obligatorio responder.
	// (Si quieres que el emisor registre fallos asíncronos, responde con
	// Status:false y lo verá en su log.)
	SUBNX.GlobalEvents.AddEvent("telemetria", func(event *SUBNX.Event, conn *SUBNX.Client) {
		var metrics map[string]string
		if err := event.Bind(&metrics); err != nil {
			log.Printf("[%s] telemetría ilegible: %v", workerName, err)
			return
		}
		log.Printf("[%s] telemetría UID=%s datos=%v", workerName, event.UID, metrics)
	})

	// ------------------------------------------------------------------
	// EVENTO LENTO: "reporte_lento" (para demostrar timeouts)
	// ------------------------------------------------------------------
	SUBNX.GlobalEvents.AddEvent("reporte_lento", func(event *SUBNX.Event, conn *SUBNX.Client) {
		var req struct {
			Seconds int `json:"seconds"`
		}
		if err := event.Bind(&req); err != nil || req.Seconds <= 0 {
			event.Reply(conn, SUBNX.State{Status: false, Message: "seconds inválido", Error: "INVALID_PAYLOAD"})
			return
		}
		log.Printf("[%s] generando reporte de %ds...", workerName, req.Seconds)
		time.Sleep(time.Duration(req.Seconds) * time.Second)
		event.Reply(conn, SUBNX.State{
			Status:  true,
			Message: "Reporte listo",
			Data:    map[string]any{"handled_by": workerName, "seconds": req.Seconds},
		})
	})

	// ------------------------------------------------------------------
	// EVENTO QUE SIEMPRE FALLA: "fallo" (para demostrar Status=false)
	// ------------------------------------------------------------------
	SUBNX.GlobalEvents.AddEvent("fallo", func(event *SUBNX.Event, conn *SUBNX.Client) {
		event.Reply(conn, SUBNX.State{
			Status:  false,
			Message: "Fallo simulado",
			Error:   "SIMULATED_FAILURE",
		})
	})
}
