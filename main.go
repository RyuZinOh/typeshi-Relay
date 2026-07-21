package main

import (
	"encoding/json"
	"log"
	"net/http"
	"os"
	"sync"

	"github.com/gorilla/websocket"
	"github.com/joho/godotenv"
)

var upgrader = websocket.Upgrader{
	CheckOrigin: func(r *http.Request) bool { return true },
}

const maxPlayersPerRoom = 2

type Room struct {
	mu      sync.Mutex
	clients map[*websocket.Conn]string
	code    string
}

var (
	roomsMu sync.Mutex
	rooms   = make(map[string]*Room)
)

type Envelope struct {
	Type     string `json:"type"`
	Room     string `json:"room"`
	Username string `json:"username"`
}

func getOrCreateRoom(code string) *Room {
	roomsMu.Lock()
	defer roomsMu.Unlock()
	r, ok := rooms[code]
	if !ok {
		r = &Room{clients: make(map[*websocket.Conn]string), code: code}
		rooms[code] = r
	}
	return r
}

func handleWS(w http.ResponseWriter, r *http.Request) {
	conn, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		log.Println("upgrade failed: ", err)
		return
	}
	log.Println("Client connected: ", conn.RemoteAddr())
	defer conn.Close()

	var room *Room

	defer func() {
		if room == nil {
			return
		}
		room.mu.Lock()
		delete(room.clients, conn)
		empty := len(room.clients) == 0
		room.mu.Unlock()

		if empty {
			roomsMu.Lock()
			delete(rooms, room.code)
			roomsMu.Unlock()
			log.Println("room emptied and removed: ", room.code)
		}
	}()

	for {
		msgType, msg, err := conn.ReadMessage()
		if err != nil {
			log.Println("Client disconnected: ", conn.RemoteAddr())
			return
		}
		var env Envelope
		if err := json.Unmarshal(msg, &env); err != nil {
			log.Println("bad message: ", err)
			continue
		}

		if env.Type == "join" {
			if env.Username == "" {
				conn.WriteMessage(websocket.TextMessage, []byte(`{"type":"error","message":"username required"}`))
				log.Println("join rejected: no username")
				continue
			}
			if env.Room == "" {
				conn.WriteMessage(websocket.TextMessage, []byte(`{"type":"error","message":"room required"}`))
				log.Println("join rejected: no room")
				continue
			}

			r := getOrCreateRoom(env.Room)
			r.mu.Lock()
			if len(r.clients) >= maxPlayersPerRoom {
				r.mu.Unlock()
				conn.WriteMessage(websocket.TextMessage, []byte(`{"type":"error","message":"room full"}`))
				log.Println("join rejected: room full: ", env.Room)
				continue
			}
			r.clients[conn] = env.Username
			r.mu.Unlock()

			room = r
			log.Println("client joined room: ", env.Room, "as", env.Username)
			continue
		}

		if room == nil {
			log.Println("message before join, ignoring")
			continue
		}

		room.mu.Lock()
		username := room.clients[conn]

		var payload map[string]interface{}
		if err := json.Unmarshal(msg, &payload); err != nil {
			room.mu.Unlock()
			log.Println("bad broadcast payload: ", err)
			continue
		}
		payload["username"] = username
		outbound, _ := json.Marshal(payload)

		for c := range room.clients {
			if c == conn {
				continue
			}
			if err := c.WriteMessage(msgType, outbound); err != nil {
				log.Println("broadcast write failed, ", err)
			}
		}
		room.mu.Unlock()
		log.Println("received from", username, ": ", string(msg))
	}
}

func main() {
	godotenv.Load()
	port := os.Getenv("PORT")
	http.HandleFunc("/ws", handleWS)
	log.Println("listening to port: " + port)
	log.Fatal(http.ListenAndServe(":"+port, nil))
}
