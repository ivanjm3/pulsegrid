// Command api runs the Pulsegrid API server.
package main

import (
	"log"
	"net/http"

	"pulsegrid/pkg/api"
)

func main() {
	mux := http.NewServeMux()
	mux.Handle("/videos/upload", api.NewUploadHandler())

	const addr = ":8080"
	log.Printf("pulsegrid api server listening on %s", addr)
	if err := http.ListenAndServe(addr, mux); err != nil {
		log.Fatal(err)
	}
}
