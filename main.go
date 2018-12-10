package main

import (
	"context"
	"flag"
	"log"
	"net/http"
	_ "net/http/pprof"
	"os"
	"os/signal"
	"syscall"
	"time"
)

func main() {
	listen := flag.String("listen", ":8080", "Address to listen on")
	pprof := flag.String("pprof", "", "To enable pprof, pass address to listen such as 'localhost:6060'")
	expirationDurationStr := flag.String("expiration-duration", "10m", "Duration to expire samples")
	flag.Parse()

	if *pprof != "" {
		go func() {
			log.Printf("Enabling pprof on %s", *pprof)
			log.Println(http.ListenAndServe(*pprof, nil))
		}()
	}

	expirationDuration, err := time.ParseDuration(*expirationDurationStr)
	if err != nil {
		log.Fatal(err)
	}

	s, err := NewServer(expirationDuration)
	if err != nil {
		log.Fatal(err)
	}

	log.Printf("Listening %s", *listen)
	srv := &http.Server{
		Addr:    *listen,
		Handler: s,
	}

	go func() {
		err = srv.ListenAndServe()
		if err != nil && err != http.ErrServerClosed {
			log.Fatal(err)
		}
	}()

	termCh := make(chan os.Signal)
	signal.Notify(termCh, syscall.SIGTERM)
	<-termCh
	log.Printf("Shutting down")

	ctx, cancel := context.WithTimeout(context.Background(), time.Minute*5)
	defer cancel()
	err = srv.Shutdown(ctx)
	if err != nil {
		log.Fatalf("Error shutting down HTTP server: %s", err)
	}
	log.Printf("Successfully shutted down")
}
