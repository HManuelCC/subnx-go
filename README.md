# SUBNX go

Demo completa de cómo usar el driver de Go de **SUBNX**: un balanceador/orquestador
tipo pub/sub donde varios nodos que se suscriben al mismo evento forman un _grid_
(un servidor distribuido en red) y el balanceador decide cuál atiende cada petición.

- Driver (SDK): [`github.com/HManuelCC/subnx-go`](https://github.com/HManuelCC/subnx-go)
- Servidor SUBNX: se distribuye como ejecutable y se corre aparte.
  Este repo contiene el **driver de Go** y ejemplos de uso en `cmd/`.
- Driver de Python: ya está en PyPI, `pip install subnx`.

## Instalación

```bash
go get github.com/HManuelCC/subnx-go
```

```go
import SUBNX "github.com/HManuelCC/subnx-go"
```

## Correr los ejemplos

Con el servidor SUBNX corriendo:

```bash
git clone https://github.com/HManuelCC/subnx-go.git
cd subnx-go

go run ./cmd/demo/worker/main.go -name worker-1
go run ./cmd/demo/worker/main.go -name worker-2
go run ./cmd/demo/sender/main.go
go run ./cmd/demo/gateway/main.go
```

```bash
curl -X POST localhost:8080/login -d '{"username":"manuel","password":"1234"}'   # 200
curl -X POST localhost:8080/login -d '{"username":"","password":""}'             # 400 (Status=false)
curl -X POST localhost:8080/telemetry -d '{"cpu":"45%","ram":"2GB"}'             # 202 fire-and-forget
curl 'localhost:8080/reporte?seconds=3&timeout=1'                                 # 504 timeout
curl localhost:8080/fallo                                                         # 400
```

## Cómo se usa el driver

**Worker: se registran los eventos antes de conectar**

```go
SUBNX.GlobalEvents.AddEvent("login", func(event *SUBNX.Event, conn *SUBNX.Client) {
    var creds struct {
        Username string `json:"username"`
        Password string `json:"password"`
    }
    if err := event.Bind(&creds); err != nil {
        event.Reply(conn, SUBNX.State{Status: false, Error: "INVALID_PAYLOAD"})
        return
    }
    event.Reply(conn, SUBNX.State{Status: true, Message: "ok", Data: map[string]any{"user": creds.Username}})
})

client := SUBNX.NewClient("host", "port", "client_name", "client_api_key", false)
client.Listen() // bloquea; apaga limpio con Ctrl+C
```

**Cliente: enviar un evento y esperar la respuesta**

```go
d := 5 * time.Second // *time.Duration; nil = default (65 s)

err := client.Send(&SUBNX.Event{
    Event: "login",
    Data:  map[string]string{"username": "manuel", "password": "1234"},
}, &d, func(resp SUBNX.State) {
    fmt.Println(resp.Message) // solo se ejecuta con Status=true
})
if err != nil {
    // Status=false del worker, timeout o error de red
}
```

## Bind: tus tipos son el contrato

En SUBNX no hay un esquema central ni un contrato declarado en el servidor. El
**tipo que tú pasas a `Bind` es el contrato**: si sus atributos JSON coinciden con los
datos que se enviaron, se rellena; si no son compatibles, `Bind` devuelve un error.

- Solo importan los **tags `json`**, no el nombre del struct ni el de los campos Go.
- Existe en los dos lados: `event.Bind(&x)` al **recibir** una petición y
  `resp.Bind(&x)` al **leer la respuesta** (`State.Data`).
- Siempre revisa el error que devuelve.

```go
// Quien recibe el evento (worker)
var creds struct {
    Username string `json:"username"`
    Password string `json:"password"`
}
err := event.Bind(&creds)

// Quien lee la respuesta (cliente): mismo mecanismo
err := client.Send(&ev, nil, func(resp SUBNX.State) {
    var out struct {
        Token     string `json:"token"`
        HandledBy string `json:"handled_by"`
    }
    if err := resp.Bind(&out); err != nil {
        return
    }
    fmt.Println(out.Token, out.HandledBy)
})
```

Si el emisor mandó `{"username":"Humberto","password":"1244"}`, cualquier struct con
los tags `username` y `password` sirve como receptor.

## Reglas del SDK

| Tema           | Comportamiento                                                                                                                                           |
| -------------- | -------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Orden          | `GlobalEvents.AddEvent(...)` **antes** de `NewClient(...)`                                                                                               |
| Conexión       | `NewClient` devuelve el cliente de inmediato; el handshake ocurre en segundo plano (los errores salen en consola). Por ahora se espera con `time.Sleep`  |
| `Send`         | Bloquea hasta que responde un worker o vence el timeout. El callback corre en la misma goroutine, así que es seguro escribir en un `http.ResponseWriter` |
| `Status=true`  | Se ejecuta el callback y `Send` devuelve `nil`                                                                                                           |
| `Status=false` | **No** se ejecuta el callback; `Send` devuelve el error                                                                                                  |
| Sin callback   | Fire-and-forget: `Send` regresa en cuanto escribe el paquete                                                                                             |
| Datos          | Sin contrato central. `Bind(&T)` rellena cualquier tipo con tags JSON compatibles; el nombre del struct y de los campos Go no importa                    |
| `State.Data`   | `interface{}`: la librería lo serializa. En el cliente se lee con `resp.Bind(&out)`                                                                      |
| Grid           | Varios clientes con distinto `client_name` y el mismo `AddEvent` comparten la carga                                                                      |
