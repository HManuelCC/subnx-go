package orvanta

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net"
	"time"

	"github.com/google/uuid"
)

func (e *Event) SendData(client *Client, timeout *time.Duration, response ...ResponseCallback) error {
	e.UID = generateUID()

	err := client.writePacket(1, e.Event, "", e)
	if err != nil {
		return err
	}

	req := &pendingRequest{
		UID:  e.UID,
		Data: make(chan State, 1),
	}
	client.pendingRequests.Store(e.UID, req)

	if timeout == nil {
		defaultTimeout := 65 * time.Second
		timeout = &defaultTimeout
	}

	if len(response) > 0 {
		select {
		case state := <-req.Data:
			if state.Status {
				response[0](state)
				return nil
			}
			return fmt.Errorf("error del servidor: %s", state.Error)
		case <-time.After(*timeout):
			client.pendingRequests.Delete(e.UID)
			return fmt.Errorf("timeout esperando respuesta para UID %s", e.UID)
		}
	} else {
		go func() {
			select {
			case state := <-req.Data:
				if !state.Status {
					log.Println("Respuesta asíncrona fallida:", state.Error)
				}
			case <-time.After(*timeout):
				client.pendingRequests.Delete(e.UID)
			}
		}()
	}
	return nil
}

func (e *Event) Reply(client *Client, state State) {
	e.replyOnce.Do(func() {
		if e.replied != nil {
			close(e.replied)
		}
	})

	state.UID = e.UID
	client.writePacket(2, "", e.ServerUID, state)
}

func (m MessageState) send(client *Client) {
	client.writePacket(3, "", m.ServerUID, m)
}

func (c *Client) readLoop(serverStatus chan bool, latency float64) {
	for {
		// 1. LEER PREFIJO FIJO (42 Bytes)
		prefix := make([]byte, 42)
		_, err := io.ReadFull(c.Conn, prefix)
		if err != nil {
			if err == io.EOF {
				serverStatus <- false
				return
			}
			netErr, ok := err.(net.Error)
			if ok && netErr.Temporary() {
				continue
			}
			serverStatus <- false
			return
		}

		msgType := prefix[0]
		nameLen := int(prefix[1])

		uidBytes := bytes.Trim(prefix[2:38], "\x00")
		messageUid := string(uidBytes)

		payloadSize := binary.BigEndian.Uint32(prefix[38:42])

		// 2. LEER CUERPO DINÁMICO
		body := make([]byte, nameLen+int(payloadSize))
		if _, err = io.ReadFull(c.Conn, body); err != nil {
			continue
		}

		// 3. SEPARAMOS LOS DATOS
		eventName := string(body[:nameLen])
		data := body[nameLen:]

		switch msgType {
		case 1:
			var incoming struct {
				Event string          `json:"event"`
				UID   string          `json:"uid"`
				Data  json.RawMessage `json:"data"` // ¡Forzamos bytes crudos!
			}

			if err := json.Unmarshal(data, &incoming); err != nil {
				continue
			}

			if eventName != "" {
				incoming.Event = eventName
			}

			// Trasladamos los datos al Event real de forma limpia
			event := Event{
				Event:     incoming.Event,
				UID:       incoming.UID,
				ServerUID: messageUid,
				rawData:   incoming.Data, // Guardamos los bytes en secreto
			}

			ack := MessageState{Status: true, ServerUID: messageUid, ProcessStatus: 1, Message: "ACK recibido Evento"}
			ack.send(c)

			go HandleEvents(&event, c, c.name, int(latency))

		case 2:
			var incoming struct {
				Status  bool            `json:"status"`
				Message string          `json:"message"`
				Error   string          `json:"error"`
				UID     string          `json:"uid"`
				Data    json.RawMessage `json:"data"` // ¡Forzamos bytes crudos!
			}

			if err := json.Unmarshal(data, &incoming); err != nil {
				continue
			}

			state := State{
				Status:    incoming.Status,
				Message:   incoming.Message,
				Error:     incoming.Error,
				UID:       incoming.UID,
				ServerUID: messageUid,
				rawData:   incoming.Data, // Guardamos los bytes en secreto
			}

			ack := MessageState{Status: true, ServerUID: messageUid, ProcessStatus: 2, Message: "ACK recibido State"}
			ack.send(c)

			if val, ok := c.pendingRequests.LoadAndDelete(state.UID); ok {
				req := val.(*pendingRequest)
				req.Data <- state
				close(req.Data)
			} else {
				log.Println("Proceso no encontrado para State UID:", state.UID)
			}
		default:
			log.Println("No se espera recibir ACKs en el cliente. Ignorando...")
		}
	}
}

func generateUID() string {
	return uuid.New().String()
}
