package main

import (
	"log"
	"net/http"
	"os"

	"github.com/gorilla/websocket"
	"github.com/joho/godotenv"
)

var upgrader = websocket.Upgrader{
	CheckOrigin: func(r *http.Request) bool { return true },
}

func handleWS(w http.ResponseWriter, r *http.Request) {
	conn, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		log.Println("upgrade failed: ", err)
		return
	}
	log.Println("Client connected: ", conn.RemoteAddr())
	defer conn.Close()

	for {
		msgType, msg, err := conn.ReadMessage()
		if err != nil {
			log.Println("Client disconnected: ", conn.RemoteAddr())
			return
		}
		log.Println("received: ", string(msg))
		conn.WriteMessage(msgType, msg)
	}
}

func main() {
	godotenv.Load()
	port := os.Getenv("PORT")
	http.HandleFunc("/ws", handleWS)
	log.Println("listening to port: " + port)
	log.Fatal(http.ListenAndServe(":"+port, nil))
}
