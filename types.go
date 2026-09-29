package orvanta

import (
	"encoding/json"
	"fmt"
	"sync"
)

type ClientHardwareResourcesStatistics struct {
	CPUUsage    float64
	MemoryUsage float64
	DiskUsage   float64
	DiskBusy    float64
}

type ClientInformation struct {
	ClientName string                            `json:"client_name"`
	ApiKey     string                            `json:"api_key"`
	Latency    float64                           `json:"latency"`
	Resources  ClientHardwareResourcesStatistics `json:"resources"`
	Events     EventsSubscribed                  `json:"events"`
}

type EventsSubscribed struct {
	Events []string `json:"events"`
}

type ResponseCallback func(response State)

type Event struct {
	Event     string          `json:"event"`
	UID       string          `json:"uid"`
	ServerUID string          `json:"-"`
	replied   chan struct{}   `json:"-"`
	extended  chan struct{}   `json:"-"`
	replyOnce sync.Once       `json:"-"`
	Data      interface{}     `json:"data"`
	rawData   json.RawMessage `json:"-"`
}

type State struct {
	Status    bool            `json:"status"`
	Message   string          `json:"message"`
	Error     string          `json:"error"`
	UID       string          `json:"uid"`
	ServerUID string          `json:"-"`
	Data      interface{}     `json:"data"`
	rawData   json.RawMessage `json:"-"`
}

type MessageState struct {
	Status        bool   `json:"status"`
	ServerUID     string `json:"server_uid"`
	Message       string `json:"state"`
	Error         string `json:"error"`
	ProcessStatus int    `json:"process_status"`
}

// pendingRequest es de uso interno para rastrear las respuestas esperadas
type pendingRequest struct {
	UID  string
	Data chan State
}

func (s *State) Bind(target interface{}) error {
	if len(s.rawData) == 0 {
		return nil // No venía nada en Data
	}
	return json.Unmarshal(s.rawData, target)
}

func (e *Event) Bind(target interface{}) error {
	if len(e.rawData) == 0 {
		return nil // No venía nada en Data
	}
	return json.Unmarshal(e.rawData, target)
}

func (s State) ToString() string {
	return fmt.Sprintf("State{Status: %v, Message: %q, Error: %q, Data: %v, UID: %q}",
		s.Status, s.Message, s.Error, s.Data, s.UID)
}

func (m MessageState) ToString() string {
	return fmt.Sprintf("MessageState{Status: %v, ServerUID: %q, Message: %q, Error: %q}",
		m.Status, m.ServerUID, m.Message, m.Error)
}
