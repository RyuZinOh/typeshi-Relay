package main

import (
	"encoding/json"
	"log"
	"math/rand/v2"
	"net/http"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/gorilla/websocket"
	"github.com/joho/godotenv"
)

var upgrader = websocket.Upgrader{
	CheckOrigin: func(r *http.Request) bool { return true },
}

const maxPlayersPerRoom = 2
const roomCodeLength = 4

type Room struct {
	mu      sync.Mutex
	clients map[*websocket.Conn]string
	code    string
	started bool
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
type StartEnvelope struct {
	Type     string `json:"type"`
	Seed     int64  `json:"seed"`
	StartAt  int64  `json:"startAt"`
	Duration int    `json:"duration"`
}
type LeftEnvelope struct {
	Type     string `json:"type"`
	Username string `json:"username"`
}

func generateRoomCode() string {
	roomsMu.Lock()
	defer roomsMu.Unlock()

	for {
		var sb strings.Builder
		for range roomCodeLength {
			sb.WriteString(strconv.Itoa(rand.IntN(10)))
		}
		code := sb.String()
		if _, exists := rooms[code]; !exists {
			return code
		}
	}
}

func createRoom() *Room {
	code := generateRoomCode()
	r := &Room{clients: make(map[*websocket.Conn]string), code: code}

	roomsMu.Lock()
	rooms[code] = r
	roomsMu.Unlock()

	return r
}

func getRoom(code string) (*Room, bool) {
	roomsMu.Lock()
	defer roomsMu.Unlock()
	r, ok := rooms[code]
	return r, ok
}

func broadcastStart(r *Room) {
	seed := int64(rand.Int32())
	startAt := time.Now().UnixMilli() + 3000

	env := StartEnvelope{
		Type:     "start",
		Seed:     seed,
		StartAt:  startAt,
		Duration: 60,
	}
	data, _ := json.Marshal(env)
	r.mu.Lock()
	r.started = true
	for c := range r.clients {
		c.WriteMessage(websocket.TextMessage, data)
	}
	r.mu.Unlock()

	log.Println("race starting in room: ", r.code, "seed:", seed)
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
		leavingUsername := room.clients[conn]
		log.Println("cleanup: leaving username =", leavingUsername, "room =", room.code)
		delete(room.clients, conn)
		empty := len(room.clients) == 0
		// room.mu.Unlock()

		if !empty {
			env := LeftEnvelope{
				Type:     "opponent_left",
				Username: leavingUsername,
			}

			data, _ := json.Marshal(env)
			log.Println("broadcasting opponent_left payload:", string(data))
			for c := range room.clients {
				c.WriteMessage(websocket.TextMessage, data)
			}
		}
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

		if env.Type == "create" {
			if env.Username == "" {
				conn.WriteMessage(websocket.TextMessage, []byte(`{"type":"error","message":"username required"}`))
				log.Println("create rejected: no username")
				continue
			}

			r := createRoom()
			r.mu.Lock()
			r.clients[conn] = env.Username
			r.mu.Unlock()

			room = r
			log.Println("room created: ", r.code, "by", env.Username)

			var sb strings.Builder
			sb.WriteString(`{"type":"created","room":"`)
			sb.WriteString(r.code)
			sb.WriteString(`"}`)
			conn.WriteMessage(websocket.TextMessage, []byte(sb.String()))
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

			r, exists := getRoom(env.Room)
			if !exists {
				conn.WriteMessage(websocket.TextMessage, []byte(`{"type":"error","message":"room not found"}`))
				log.Println("join rejected: room not found: ", env.Room)
				continue
			}

			r.mu.Lock()
			if r.started {
				r.mu.Unlock()
				conn.WriteMessage(websocket.TextMessage, []byte(`{"type":"error","message":"race already in progress"}`))
				log.Println("join rejected: race already started: ", env.Room)
				continue
			}
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

			room.mu.Lock()
			var jb strings.Builder
			jb.WriteString(`{"type":"player_joined","username":"`)
			jb.WriteString(env.Username)
			jb.WriteString(`"}`)
			joinedMsg := jb.String()
			for c := range room.clients {
				if c == conn {
					continue
				}
				c.WriteMessage(websocket.TextMessage, []byte(joinedMsg))
			}
			room.mu.Unlock()

			conn.WriteMessage(websocket.TextMessage, []byte(`{"type":"joined"}`))
			room.mu.Lock()
			full := len(room.clients) >= maxPlayersPerRoom
			room.mu.Unlock()

			if full {
				broadcastStart(room)
			}

			continue
		}

		if env.Type == "race_started" {
			if room != nil {
				room.mu.Lock()
				room.started = true
				room.mu.Unlock()
			}
			continue
		}

		if room == nil {
			log.Println("message before join, ignoring")
			continue
		}

		room.mu.Lock()
		username := room.clients[conn]

		var payload map[string]any
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
