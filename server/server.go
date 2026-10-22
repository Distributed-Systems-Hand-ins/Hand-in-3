package main

import (
	"context"
	"fmt"
	"io"
	"log"
	"net"
	"os"
	"os/signal"
	"sync"
	"unicode/utf8"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	proto "chatroom/grpc"
)

// Pool is a struct that holds the state of the server and manages the connected clients.
type Pool struct {
	proto.UnimplementedBroadcastServer

	mu      sync.Mutex                     // guards clock and clients
	clock   int64                          // the server's Lamport clock
	clients map[string]chan *proto.Message // participant name -> outbox
	closing bool						   // true once the server is shutting down
}

func main() {
	// Log to both the terminal and server.log file
	logFile, err := os.OpenFile("server.log", os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0666)
	if err != nil {
		log.Fatalf("Failed to open log file: %v", err)
	}
	defer logFile.Close()
	log.SetOutput(io.MultiWriter(os.Stdout, logFile))

	//Create a server pool to manage connected clients
	grpcServer := grpc.NewServer()
	pool := &Pool{clients: make(map[string]chan *proto.Message)}
	proto.RegisterBroadcastServer(grpcServer, pool)

	// Listen for incoming connections on port 8080
	listener, err := net.Listen("tcp", "localhost:8080")
	if err != nil {
		log.Fatalf("Failed to listen on port 8080: %v", err)
	}
	stop := make(chan os.Signal, 1)
	signal.Notify(stop, os.Interrupt)
	go func() {
		<-stop
		pool.mu.Lock()
		pool.closing = true
		pool.mu.Unlock()
		log.Printf("[Server] event=shutdown")
		grpcServer.Stop()
	}()

	// Start serving incoming connections
	log.Printf("[Server] event=startup address=%s", listener.Addr())
	if err := grpcServer.Serve(listener); err != nil {
		log.Fatalf("[Server] event=error err=%v", err)
	}
}

// Broadcast puts msg in every participant's outbox. Must be called with p.mu held.
// The send never blocks: if a participant's outbox is full the message is dropped for them.
func (p *Pool) Broadcast(msg *proto.Message) {
	for name, outbox := range p.clients {
		select {
		case outbox <- msg:
		default:
			log.Printf("[Server] event=drop client=%s lamport=%d reason=outbox_full", name, msg.Lamport)
		}
	}
}

func (p *Pool) CreateStream(req *proto.Connect, stream proto.Broadcast_CreateStreamServer) error {
	name := req.GetUser().GetName()
	if name == "" {
		return status.Error(codes.InvalidArgument, "name cannot be empty")
	}
	outbox := make(chan *proto.Message, 100) // buffered channel to avoid blocking

	// Join: Add the participant first, so they also get their own join message
	p.mu.Lock()
	if _, taken := p.clients[name]; taken {
		p.mu.Unlock()
		return status.Error(codes.AlreadyExists, "name already taken")
	}

	p.clock = max(p.clock, req.Lamport) + 1 // receive event, join request
	p.clients[name] = outbox
	log.Printf("[Server] event=connect client=%s lamport=%d", name, p.clock)
	p.clock++ // Send event, join response
	p.Broadcast(&proto.Message{
		Sender:  "Server",
		Content: fmt.Sprintf("Participant %s joined Chit Chat at logical time %d", name, p.clock),
		Lamport: p.clock,
	})
	log.Printf("[Server] event=broadcast_join client=%s lamport=%d", name, p.clock)
	p.mu.Unlock()

	// Leave: runs when the participant leaves the chatroom
	defer func() {
		p.mu.Lock()
		defer p.mu.Unlock()
		if p.closing {
			return // server is shutting down, nobody is left to tell
		}
		delete(p.clients, name) // Remove first, so they don't get their own leave message
		log.Printf("[Server] event=disconnect client=%s", name)
		p.clock++ // Send event, leave message
		p.Broadcast(&proto.Message{
			Sender:  "Server",
			Content: fmt.Sprintf("Participant %s left Chit Chat at logical time %d", name, p.clock),
			Lamport: p.clock,
		})
		log.Printf("[Server] event=broadcast_leave client=%s lamport=%d", name, p.clock)
	}()

	// Deliver: this goroutine delivers messages to the participant's stream
	for {
		select {
		case msg := <-outbox:
			if err := stream.Send(msg); err != nil {
				return err
			}
			log.Printf("[Server] event=deliver client=%s sender=%s lamport=%d", name, msg.Sender, msg.Lamport)
		case <-stream.Context().Done():
			return nil // The participant disconnected
		}
	}
}

// BroadcastMessage is called by a participant to send a message to all participants
func (p *Pool) BroadcastMessage(ctx context.Context, msg *proto.Message) (*proto.Close, error) {
	// A valid message is UTF-8 and at most 128 characters
	if !utf8.ValidString(msg.Content) || utf8.RuneCountInString(msg.Content) > 128 {
		log.Printf("[Server] event=reject client=%s reason=invalid_message", msg.Sender)
		return nil, status.Error(codes.InvalidArgument, "messages must be UTF-8 and at most 128 characters")
	}

	p.mu.Lock()
	defer p.mu.Unlock()
	if _, joined := p.clients[msg.Sender]; !joined {
		log.Printf("[Server] event=reject client=%s reason=not_joined", msg.Sender)
		return nil, status.Error(codes.PermissionDenied, "you have not joined Chit Chat")
	}
	p.clock = max(p.clock, msg.Lamport) + 1 // receive event, broadcast request
	p.clock++                               // send event, broadcast message
	p.Broadcast(&proto.Message{
		Sender:  msg.Sender,
		Content: msg.Content,
		Lamport: p.clock,
	})
	log.Printf("[Server] event=publish client=%s lamport=%d", msg.Sender, p.clock)
	return &proto.Close{}, nil
}
