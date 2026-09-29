package orvanta

import "sync"

// EventHandler define la firma limpia para el desarrollador
type EventHandler func(event *Event, conn *Client)

// EventRouter maneja las suscripciones de forma segura
type EventRouter struct {
	mu       sync.RWMutex
	handlers map[string]EventHandler
}

// GlobalEvents es el enrutador principal del SDK
var GlobalEvents = &EventRouter{
	handlers: make(map[string]EventHandler),
}

func (r *EventRouter) AddEvent(eventName string, handler EventHandler) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.handlers[eventName] = handler
}

func (r *EventRouter) RemoveEvent(eventName string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	delete(r.handlers, eventName)
}

func (r *EventRouter) GetRegisteredEventNames() []string {
	r.mu.RLock()
	defer r.mu.RUnlock()
	var names []string
	for name := range r.handlers {
		names = append(names, name)
	}
	return names
}

func (r *EventRouter) GetHandler(eventName string) (EventHandler, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	handler, exists := r.handlers[eventName]
	return handler, exists
}
