// Implementation of a KeyValueServer. Students should write their code in this file.

package p0partA

import (
	"bufio"
	"fmt"
	"net"
	"strings"

	"github.com/cmu440/p0partA/kvstore"
)

const (
	AddClient    = 1
	DropClient   = 2
	CheckActive  = 3
	CheckDropped = 4
	Get          = 5
	Put          = 6
	Delete       = 7
	Update       = 8
	bufferSize   = 500
)

type keyValueServer struct {
	kvStore        kvstore.KVStore
	activeClients  int
	droppedClients int
	listener       net.Listener
	stopChannel    chan bool    // Manage stop signals
	requestChannel chan request // Send all requests so that can handle racing conditions
	addDropChannel chan int     // Signals for add or drop a client
	checkChannel   chan int     // Channel for all check requests for active or dropped clients
	replyChannel   chan int     // Reply to checkChannel
}

type request struct {
	operation       int
	key             string
	value           []byte
	newValue        []byte
	responseChannel chan []byte // Channel for responding get request
}

// New creates and returns (but does not start) a new KeyValueServer.
// Your keyValueServer should use `store kvstore.KVStore` internally.
func New(store kvstore.KVStore) KeyValueServer {
	return &keyValueServer{
		kvStore:        store,
		stopChannel:    make(chan bool),
		requestChannel: make(chan request),
		addDropChannel: make(chan int),
		checkChannel:   make(chan int),
		replyChannel:   make(chan int),
	}
}

func (kvs *keyValueServer) Start(port int) error {
	var err error
	kvs.listener, err = net.Listen("tcp", fmt.Sprintf(":%d", port))
	if err != nil {
		return err
	}
	// In different goroutines to keep track of client number, including add drop and checks, also to process requests tostorage
	go kvs.manageClientNumber()
	go kvs.KVSProcessRequest()
	go kvs.acceptConnections()

	return nil
}

// acceptConnections handles incoming client connections in a loop.
func (kvs *keyValueServer) acceptConnections() {
	for {
		conn, err := kvs.listener.Accept()
		if err != nil {
			return
		}
		go kvs.handleClient(conn) // Handle each client connection in a goroutine
	}
}

func (kvs *keyValueServer) manageClientNumber() {
	for {
		select {
		case operation := <-kvs.addDropChannel:
			switch operation {
			case AddClient:
				kvs.activeClients++
			case DropClient:
				kvs.droppedClients++
				kvs.activeClients--
			}
		case check := <-kvs.checkChannel:
			switch check {
			case CheckActive:
				kvs.replyChannel <- kvs.activeClients
			case CheckDropped:
				kvs.replyChannel <- kvs.droppedClients
			}
		case stop := <-kvs.stopChannel:
			if stop {
				fmt.Println("stopped")
				return
			}
		default:
			continue
		}
	}
}

// handleClient handles communication with a connected client.
func (kvs *keyValueServer) handleClient(conn net.Conn) {
	defer conn.Close()
	kvs.addDropChannel <- AddClient

	//set up channel to write responses if the request is a get request
	responseChannel := make(chan []byte, bufferSize)
	go kvs.writeBack(conn, responseChannel)

	reader := bufio.NewReader(conn)
	for {
		request, err := reader.ReadString('\n')
		if err != nil {
			kvs.addDropChannel <- DropClient
			close(responseChannel)
			return
		}
		request = strings.TrimSpace(request)
		kvs.processRequest(request, responseChannel)
	}
}

func (kvs *keyValueServer) writeBack(conn net.Conn, channel chan []byte) {
	for res := range channel {
		conn.Write(res)
	}
}

// processRequest parses and send requests to the single goroutine to process
func (kvs *keyValueServer) processRequest(req string, responseChannel chan []byte) {
	// Split the request to identify the operation and arguments
	parts := strings.Split(req, ":")
	switch parts[0] {
	case "Put":
		// Format: Put:key:value
		if len(parts) == 3 {
			key, value := parts[1], []byte(parts[2])
			kvs.requestChannel <- request{operation: Put, key: key, value: value}
		}
	case "Get":
		// Format: Get:key
		if len(parts) == 2 {
			key := parts[1]
			kvs.requestChannel <- request{operation: Get, key: key, responseChannel: responseChannel}
		}
	case "Delete":
		// Format: Delete:key
		if len(parts) == 2 {
			key := parts[1]
			kvs.requestChannel <- request{operation: Delete, key: key}
		}
	case "Update":
		// Format: Update:key:oldValue:newValue
		if len(parts) == 4 {
			key, oldValue, newValue := parts[1], []byte(parts[2]), []byte(parts[3])
			kvs.requestChannel <- request{operation: Update, key: key, value: oldValue, newValue: newValue}
		}
	}
}

// Process all requests sent through requestChannel, and respond only to get requests
func (kvs *keyValueServer) KVSProcessRequest() {
	for {
		select {
		case req := <-kvs.requestChannel:
			switch req.operation {
			case Put:
				kvs.kvStore.Put(req.key, req.value)
			case Get:
				res := kvs.kvStore.Get(req.key)
				for i := 0; i < len(res); i++ {
					if len(req.responseChannel) < bufferSize {
						req.responseChannel <- []byte(fmt.Sprintf("%s:%s\n", req.key, res[i]))
					}
				}
			case Delete:
				kvs.kvStore.Delete(req.key)
			case Update:
				kvs.kvStore.Update(req.key, req.value, req.newValue)
			}
		case stop := <-kvs.stopChannel:
			if stop {
				fmt.Println("Got stop signal")
				return
			}
		}
	}
}

func (kvs *keyValueServer) Close() {
	fmt.Println("Sending out close message")
	kvs.stopChannel <- true
	kvs.listener.Close()
}

func (kvs *keyValueServer) CountActive() int {
	kvs.checkChannel <- CheckActive
	return <-kvs.replyChannel
}

func (kvs *keyValueServer) CountDropped() int {
	kvs.checkChannel <- CheckDropped
	return <-kvs.replyChannel
}
