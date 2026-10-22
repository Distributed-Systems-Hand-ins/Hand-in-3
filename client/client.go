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
	if err != nil {
		log.Fatalf("[Client %s] event=error err=%v", *name, err)
	}
	defer conn.Close()
	client := proto.NewBroadcastClient(conn)

	var clock int64

	//Join: sending the join request is a send event
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	clock++
	stream, err := client.CreateStream(ctx, &proto.Connect{User: &proto.User{Name: *name}, Lamport: clock})
	if err != nil {
		fmt.Println("Could not join", status.Convert(err).Message())
		log.Printf("[Client %s] event=error err=%q", *name, status.Convert(err).Message())
		return
	}
	log.Printf("[Client %s] event=connect lamport=%d", *name, clock)
	fmt.Println("Type a message and press Enter. Type /leave or press Ctrl+C to leave")

	//Goroutine 1: receive messages from the server and hands them to the main loop
	incoming := make(chan *proto.Message)
	disconnected := make(chan error, 1)
	go func() {
		for {
			msg, err := stream.Recv()
			if err != nil {
				disconnected <- err
				return
			}
			incoming <- msg

		}
	}()

	//Goroutine 2: Reads lines from the keyboard and hands them to the main loop
	input := make(chan string)
	go func() {
		scanner := bufio.NewScanner(os.Stdin)
		for scanner.Scan() {
			input <- scanner.Text()
		}
		close(input) // stdin closed (Ctrl+D): tell the main loop to leave
	}()

	//Ctrl+C also means leave
	interrupt := make(chan os.Signal, 1)
	signal.Notify(interrupt, os.Interrupt)

	//Main loop: handles whichever of these happens next
loop:
	for {
		select {
		case msg := <-incoming:
			clock = max(clock, msg.Lamport) + 1 //receive event

			//Show the message and its timestamp, and log them
			fmt.Printf("[%d] %s: %s\n", msg.Lamport, msg.Sender, msg.Content)
			log.Printf("[Client %s] event=receive sender=%s lamport=%d clock=%d content=%q",
				*name, msg.Sender, msg.Lamport, clock, msg.Content)
		case text, ok := <-input:
			if !ok || text == "/leave" {
				break loop
			}
			if text == "" {
				continue
			}
			clock++ //send event
			_, err := client.BroadcastMessage(ctx, &proto.Message{Sender: *name, Content: text, Lamport: clock})
			if err != nil {
				fmt.Println("Message not sent:", status.Convert(err).Message())
				log.Printf("[Client %s] event=rejected lamport=%d err=%q", *name, clock, status.Convert(err).Message())
				continue
			}
			log.Printf("[Client %s] event=publish lamport=%d content=%q", *name, clock, text)

		case <-interrupt:
			break loop

		case err := <-disconnected:
			fmt.Println("Disconnected:", status.Convert(err).Message())
			log.Printf("[Client %s] event=disconnected err=%q", *name, status.Convert(err).Message())
			return
		}
	}

	// Leave
	clock++
	log.Printf("[Client %s] event=leave lamport=%d", *name, clock)
	fmt.Println("You left Chit Chat")
}
