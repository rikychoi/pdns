package main

import (
	"encoding/json"
	"fmt"
	"os"

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

	for {
		select {
		case jq := <-stream:
			event, err := jq.Object()
			if err != nil {
				fmt.Println(err)
				continue
			}
			if err := encoder.Encode(event); err != nil {
				fmt.Println(err)
			}
		case err := <-errStream:
			fmt.Println(err)
		}
	}
}
