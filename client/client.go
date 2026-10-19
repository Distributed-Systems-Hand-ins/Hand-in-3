package main

import (
	"bufio"
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"

	proto "chatroom/grpc"
)

func main() {
	// Read the participants name
	name := flag.String("name", "", "Your name in the chatroom")
	flag.Parse()
	if *name == "" {
		fmt.Println("Usage: go run ./client -name Alice")
		return
	}

	// Log only to a file, so log lines dont get mixed with user input
	logFile, err := os.OpenFile("client-"+*name+".log", os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
	if err != nil {
		log.Fatalf("Error opening log file: %v", err)
	}
	defer logFile.Close()
	log.SetOutput(logFile)

	// Connect to the server
	conn, err := grpc.NewClient("localhost:8080", 
    grpc.WithTransportCredentials(insecure.NewCredentials()))
	}
}