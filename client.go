package orvanta

import (
	"crypto/tls"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"math/rand"
	"net"
	"os"
	"os/signal"
	"sync"
	"sync/atomic"
	"syscall"
	"time"
)

type Client struct {
	host   string
	port   string
	name   string
	ApiKey string

	mu      sync.RWMutex
	Conn    net.Conn
	WriteMu sync.Mutex

	closed  atomic.Bool
	isReady atomic.Bool
	done    chan struct{}
	attempt int

	minBackoff time.Duration
	maxBackoff time.Duration
	tlsConf    *tls.Config

	// 👈 NUEVO: Mapa seguro para procesos concurrentes
	pendingRequests sync.Map
}

func NewClient(host, port, clientName string, apiKey string, useTLS bool) *Client {
	c := &Client{
		host:       host,
		port:       port,
		name:       clientName,
		ApiKey:     apiKey,
		done:       make(chan struct{}),
		attempt:    1,
		minBackoff: 1 * time.Second,
		maxBackoff: 30 * time.Second,
		tlsConf:    &tls.Config{InsecureSkipVerify: true},
	}

	go c.Run(useTLS)
	return c
}

func (c *Client) Send(event *Event, timeout *time.Duration, cb ...ResponseCallback) error {
	if c.closed.Load() {
		return errors.New("client cerrado")
	}

	// 🌟 MAGIA DE CONCURRENCIA: Esperamos inteligentemente a que el handshake termine
	ready := false
	for i := 0; i < 50; i++ { // Espera máxima de 5 segundos
		if c.isReady.Load() && c.GetConn() != nil {
			ready = true
			break
		}
		time.Sleep(100 * time.Millisecond)
	}

	if !ready {
		return errors.New("conexión no está lista (handshake pendiente o servidor caído)")
	}

	return event.SendData(c, timeout, cb...)
}

func (c *Client) SetReady() {
	c.isReady.Store(true)
}

func (c *Client) Close() error {
	if !c.closed.CompareAndSwap(false, true) {
		return nil
	}
	close(c.done)

	c.mu.Lock()
	defer c.mu.Unlock()
	if c.Conn != nil {
		return c.Conn.Close()
	}
	return nil
}

func (c *Client) GetConn() net.Conn {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.Conn
}

func (c *Client) SetConn(conn net.Conn) {
	c.mu.Lock()
	c.Conn = conn
	c.mu.Unlock()
}

func (c *Client) ClearConn() {
	c.mu.Lock()
	if c.Conn != nil {
		_ = c.Conn.Close()
	}
	c.Conn = nil
	c.isReady.Store(false)
	c.mu.Unlock()
}

func (c *Client) Run(useTLS bool) {
	for {
		if c.closed.Load() {
			return
		}

		start := time.Now()
		var conn net.Conn
		var err error

		if useTLS {
			conn, err = tls.Dial("tcp", net.JoinHostPort(c.host, c.port), c.tlsConf)
		} else {
			conn, err = net.Dial("tcp", net.JoinHostPort(c.host, c.port))
		}

		if err != nil {
			delay := c.backoff()
			log.Printf("[client %s] error al conectar: %v (reintento en %s)", c.name, err, delay)
			select {
			case <-time.After(delay):
				c.attempt++
				continue
			case <-c.done:
				return
			}
		}

		c.attempt = 1
		handshakeLatency := time.Since(start).Milliseconds()
		log.Printf("[client %s] conectado (latencia: %d ms)", c.name, handshakeLatency)

		c.SetConn(conn)

		serverStatus := make(chan bool, 1)
		go c.readLoop(serverStatus, float64(handshakeLatency)) // Ahora ReadData le pertenece al Client

		select {
		case status := <-serverStatus:
			if !status {
				log.Printf("[client %s] desconectado por el servidor, reconectando...", c.name)
				c.ClearConn()
			}
		case <-c.done:
			c.ClearConn()
			return
		}
	}
}

func (c *Client) backoff() time.Duration {
	base := c.minBackoff << (c.attempt - 1)
	if base > c.maxBackoff {
		base = c.maxBackoff
	}
	jit := time.Duration(rand.Int63n(int64(base) / 5))
	if rand.Intn(2) == 0 {
		return base - jit
	}
	return base + jit
}

func (c *Client) writePacket(msgType byte, eventName string, serverUID string, object interface{}) error {
	data, err := json.Marshal(object)
	if err != nil {
		return fmt.Errorf("error al serializar paquete: %w", err)
	}

	// 1. Obtenemos el nombre del evento en bytes y calculamos su tamaño (Max 255)
	eventNameBytes := []byte(eventName)
	nameLen := byte(len(eventNameBytes))

	// 2. ServerUID (36 bytes fijos)
	serverUidBuffer := make([]byte, 36)
	copy(serverUidBuffer, []byte(serverUID))

	// 3. Payload Size (4 bytes)
	messageSize := uint32(len(data))
	sizeBuffer := make([]byte, 4)
	binary.BigEndian.PutUint32(sizeBuffer, messageSize)

	// 4. Armamos el Prefijo Fijo (42 bytes)
	packet := append([]byte{msgType, nameLen}, serverUidBuffer...)
	packet = append(packet, sizeBuffer...)

	// 5. Armamos el Cuerpo Dinámico (NombreEvent + Payload)
	packet = append(packet, eventNameBytes...)
	packet = append(packet, data...)

	c.WriteMu.Lock()
	defer c.WriteMu.Unlock()

	conn := c.GetConn()
	if conn == nil {
		return errors.New("conexión no disponible")
	}
	_, err = conn.Write(packet)
	return err
}

func (c *Client) Listen() {
	stop := make(chan os.Signal, 1)
	// Escuchamos Ctrl+C (Interrupt) y señales de apagado de Docker (SIGTERM)
	signal.Notify(stop, os.Interrupt, syscall.SIGTERM)

	// El programa se queda congelado aquí consumiendo 0% de CPU
	<-stop
	c.Close() // Tu SDK se encarga de limpiar sus propios sockets
}
