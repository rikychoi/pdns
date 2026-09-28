package main

import (
	"encoding/json"
	"errors"
	"log"
	"net"
	"os"
	"time"

	cs "github.com/CaliDog/certstream-go"
)

func main() {
	stream, errStream := cs.CertStreamEventStream(false)
	file, err := os.OpenFile(
		"certstream.jsonl", os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644,
	)
	if err != nil {
		panic(err)
	}
	defer file.Close()

	encoder := json.NewEncoder(file)
	statusTicker := time.NewTicker(30 * time.Second)
	defer statusTicker.Stop()

	log.SetFlags(log.Ldate | log.Ltime | log.Lmicroseconds | log.Lshortfile)
	log.Printf("CertStream collection started: output=%s", file.Name())

	eventCount := 0
	var lastEventAt time.Time

	for {
		select {
		case jq, ok := <-stream:
			if !ok {
				log.Println("event stream closed")
				return
			}

			event, err := jq.Object()
			if err != nil {
				log.Printf("failed to convert event to object: %v", err)
				continue
			}
			if err := encoder.Encode(event); err != nil {
				log.Printf("failed to save event: %v", err)
				continue
			}

			eventCount++
			lastEventAt = time.Now()

		case streamErr, ok := <-errStream:
			if !ok {
				log.Println("error stream closed")
				errStream = nil
				continue
			}

			var netErr net.Error
			if errors.As(streamErr, &netErr) {
				log.Printf(
					"network error: timeout=%t temporary=%t error=%v",
					netErr.Timeout(),
					netErr.Temporary(),
					streamErr,
				)
				continue
			}
			log.Printf("stream error: type=%T error=%v", streamErr, streamErr)

		case <-statusTicker.C:
			if lastEventAt.IsZero() {
				log.Printf("status: no events received, total=%d", eventCount)
				continue
			}
			log.Printf(
				"status: total=%d last_event=%s elapsed=%s",
				eventCount,
				lastEventAt.Format(time.RFC3339),
				time.Since(lastEventAt).Round(time.Second),
			)
		}
	}
}
