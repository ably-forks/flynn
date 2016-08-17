package main

import (
	"net/http"
	"os"

	"github.com/ably-forks/flynn/pkg/shutdown"
)

func main() {
	defer shutdown.Exit()

	port := os.Getenv("PORT")
	addr := ":" + port

	http.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte("OK"))
	})
	http.ListenAndServe(addr, nil)
}
