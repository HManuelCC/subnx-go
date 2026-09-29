package orvanta

import (
	"encoding/json"
	"fmt"
	"log"
	"time"
)

func HandleEvents(e *Event, conn *Client, clientName string, latency int) {
	if e.Event == "connect" {
		handleConnectEvent(e, conn, clientName, latency)
		return
	}

	handler, exists := GlobalEvents.GetHandler(e.Event)
	if !exists {
		log.Println("Evento no reconocido:", e.ServerUID, e.Event)
		e.Reply(conn, State{Status: false, Message: "Evento no reconocido", Error: "NOT_FOUND"})
		return
	}

	e.replied = make(chan struct{})
	e.extended = make(chan struct{})

	// 🐕 EL PERRO GUARDIÁN DE DOS FASES
	go func(evento *Event, cliente *Client) {
		select {
		case <-evento.replied:
			return // Fase 1 OK
		case <-evento.extended:
			// Prórroga activa (Fase 2)
			select {
			case <-evento.replied:
				return // Contestó durante la prórroga
			case <-time.After(28 * time.Second):
				abort := MessageState{Message: "Timeout asíncrono", Status: false, ServerUID: evento.ServerUID, Error: "WORKER_TIMEOUT_ASYNC", ProcessStatus: 2}
				abort.send(cliente)
			}
		case <-time.After(28 * time.Second):
			abort := MessageState{Message: "Worker colgado", Status: false, ServerUID: evento.ServerUID, Error: "WORKER_HANG", ProcessStatus: 2}
			abort.send(cliente)
		}
	}(e, conn)

	// 🚀 Ejecutamos al desarrollador
	handler(e, conn)

	// Validamos si contestó de inmediato o dejamos asíncrono
	select {
	case <-e.replied:
		// Todo síncrono OK
	default:
		close(e.extended) // Activar prórroga
		keepAlive := MessageState{Message: "El worker está trabajando", Status: true, ServerUID: e.ServerUID, ProcessStatus: 1}
		keepAlive.send(conn)
	}
}

func handleConnectEvent(e *Event, conn *Client, clientName string, latency int) {
	stats := &ClientHardwareResourcesStatistics{}
	err := stats.GetSystemStats()
	if err != nil {
		fmt.Println("Error obteniendo estadísticas del sistema:", err)
		return
	}

	apiKeyStr := ""
	if conn.ApiKey != "" {
		apiKeyStr = conn.ApiKey
	}

	info := &ClientInformation{
		ClientName: clientName,
		ApiKey:     apiKeyStr,
		Latency:    float64(latency),
		Resources:  *stats,
		Events:     EventsSubscribed{Events: GlobalEvents.GetRegisteredEventNames()},
	}

	jsonData, err := json.Marshal(info)
	if err != nil {
		e.Reply(conn, State{Status: false, Message: "Error interno de JSON"})
		return
	}

	e.Reply(conn, State{Status: true, Message: "Cliente conectado con exito.", Data: string(jsonData)})

	conn.SetReady()
}
