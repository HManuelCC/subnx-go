// SENDER (uso programático, sin HTTP)
//
// Recorre todos los escenarios del SDK desde código:
//  1. Envío síncrono con respuesta
//  2. Leer Data de la respuesta (interface{})
//  3. Timeout personalizado
//  4. Respuesta con Status=false (Send devuelve error)
//  5. Fire-and-forget
//  6. Ráfaga concurrente para ver el balanceo entre workers
//  7. Tarea periódica en background
//
// Levanta antes 2 o 3 workers y luego:
//
//	go run ./cmd/sender
package main

import (
	"flag"
	"fmt"
	"sort"
	"sync"
	"time"

	SUBNX "github.com/HManuelCC/subnx-go"
)

func main() {
	host := flag.String("host", "localhost", "host del servidor SUBNX")
	port := flag.String("port", "9000", "puerto del servidor SUBNX")
	name := flag.String("name", "NODO_SENDER", "nombre de este cliente")
	key := flag.String("key", "API_KEY", "api key del cliente")
	useTLS := flag.Bool("tls", false, "conectar con TLS")
	burst := flag.Int("burst", 30, "peticiones concurrentes en la ráfaga")
	flag.Parse()

	client := SUBNX.NewClient(*host, *port, *name, *key, *useTLS)

	// Sin OnConnect todavía: margen para el handshake.
	time.Sleep(2 * time.Second)

	section("1) Envío síncrono (Send bloquea hasta que responde un worker)")
	syncLogin(client)

	section("3) Timeout personalizado (reporte de 3s con timeout de 1s)")
	customTimeout(client)

	section("4) Status=false => Send devuelve error, el callback NO corre")
	failingEvent(client)

	section("5) Fire-and-forget (sin callback)")
	fireAndForget(client)

	section(fmt.Sprintf("6) Ráfaga de %d logins concurrentes (balanceo del grid)", *burst))
	concurrentBurst(client, *burst)

	section("7) Tarea periódica en background (3 pings cada 3s)")
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		periodic(client)
	}()
	wg.Wait()

	fmt.Println("\n✅ Demo terminada")
}

// 1 y 2 ----------------------------------------------------------------------
func syncLogin(client *SUBNX.Client) {
	ev := &SUBNX.Event{
		Event: "login",
		Data:  map[string]string{"username": "manuel", "password": "supersecretpassword"},
	}

	// timeout nil => default del SDK (65s).
	err := client.Send(ev, nil, func(resp SUBNX.State) {
		fmt.Printf("✅ %s\n", resp.Message)

		// 2) Data es interface{}: no sabemos si llega como map, string, etc.
		//    Re-serializar y decodificar a nuestro tipo funciona en todos los casos.
		var out struct {
			Token     string `json:"token"`
			HandledBy string `json:"handled_by"`
		}
		if err := resp.Bind(&out); err != nil {
			fmt.Println("   ⚠️ no pude leer Data:", err)
			return
		}
		fmt.Printf("   token=%s atendido por=%s\n", out.Token, out.HandledBy)
	})
	if err != nil {
		fmt.Println("❌", err)
	}
}

// 3 ---------------------------------------------------------------------------
func customTimeout(client *SUBNX.Client) {
	d := 1 * time.Second // *time.Duration: se pasa la dirección
	start := time.Now()

	err := client.Send(&SUBNX.Event{
		Event: "reporte_lento",
		Data:  map[string]int{"seconds": 3},
	}, &d, func(resp SUBNX.State) {
		fmt.Println("✅ (no debería llegar aquí):", resp.Message)
	})
	fmt.Printf("❌ esperado, tras %s: %v\n", time.Since(start).Round(time.Millisecond), err)
}

// 4 ---------------------------------------------------------------------------
func failingEvent(client *SUBNX.Client) {
	err := client.Send(&SUBNX.Event{
		Event: "fallo",
		Data:  map[string]string{},
	}, nil, func(resp SUBNX.State) {
		fmt.Println("✅ (no debería llegar aquí)")
	})
	fmt.Println("❌ esperado:", err)

	// Otro caso: validación de negocio en el worker (usuario vacío).
	err = client.Send(&SUBNX.Event{
		Event: "login",
		Data:  map[string]string{"username": "", "password": ""},
	}, nil, func(resp SUBNX.State) {})
	fmt.Println("❌ esperado:", err)
}

// 5 ---------------------------------------------------------------------------
func fireAndForget(client *SUBNX.Client) {
	err := client.Send(&SUBNX.Event{
		Event: "telemetria",
		Data:  map[string]string{"cpu_usage": "45%", "ram": "2GB"},
	}, nil) // sin callback: regresa apenas escribe el paquete
	if err != nil {
		fmt.Println("❌", err)
		return
	}
	fmt.Println("📤 telemetría enviada sin esperar respuesta")
}

// 6 ---------------------------------------------------------------------------
func concurrentBurst(client *SUBNX.Client, n int) {
	var (
		mu     sync.Mutex
		counts = map[string]int{}
		errs   int
		wg     sync.WaitGroup
	)

	start := time.Now()
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()

			err := client.Send(&SUBNX.Event{
				Event: "login",
				Data: map[string]string{
					"username": fmt.Sprintf("user%02d", i),
					"password": "pw",
				},
			}, nil, func(resp SUBNX.State) {

				var out struct {
					HandledBy string `json:"handled_by"`
				}

				_ = resp.Bind(&out)

				mu.Lock()
				counts[out.HandledBy]++
				mu.Unlock()
			})
			if err != nil {
				mu.Lock()
				errs++
				mu.Unlock()
			}
		}(i)
	}
	wg.Wait()

	fmt.Printf("⏱️  %d peticiones en %s, errores=%d\n", n, time.Since(start).Round(time.Millisecond), errs)

	workers := make([]string, 0, len(counts))
	for w := range counts {
		workers = append(workers, w)
	}
	sort.Strings(workers)
	for _, w := range workers {
		fmt.Printf("   %-12s atendió %d\n", w, counts[w])
	}
}

// 7 ---------------------------------------------------------------------------
func periodic(client *SUBNX.Client) {
	for i := 1; i <= 3; i++ {
		time.Sleep(3 * time.Second)
		i := i
		fmt.Printf("⏱️  ping %d...\n", i)
		err := client.Send(&SUBNX.Event{
			Event: "login",
			Data:  map[string]string{"username": fmt.Sprintf("cron%d", i), "password": "pw"},
		}, nil, func(resp SUBNX.State) {
			fmt.Printf("🟢 ping %d respondido: %s\n", i, resp.Message)
		})
		if err != nil {
			fmt.Printf("🔴 ping %d falló: %v\n", i, err)
		}
	}
}

// helpers ---------------------------------------------------------------------

func section(title string) {
	fmt.Printf("\n──────────────────────────────────────────────\n%s\n──────────────────────────────────────────────\n", title)
}
