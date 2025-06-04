// Contains the implementation of a LSP server.

package lsp

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/cmu440/lspnet"
)

type server struct {
	params            *Params
	conn              *lspnet.UDPConn
	nextSocketId      int
	socketEntries     map[int]*socketEntry
	addrLookup        map[string]int
	eventChan         chan *svEvent
	readChan          chan *svResult
	socketReadResults *Queue[*svResult]
	ctx               context.Context
	cancel            context.CancelCauseFunc
	ctxOp             context.Context
	cancelOp          context.CancelCauseFunc
	isPendingClose    bool
}

type svEventType int

const (
	SVETUDPReceive svEventType = iota
	SVETRequestWrite
	SVETSocketRead
	SVETCloseConn
	SVETClose
)

type svEvent struct {
	svEventType svEventType
	connID      int
	addr        *lspnet.UDPAddr
	inResult    *svResult
	outResult   chan *svResult
}

type svResult struct {
	connID int
	data   []byte
	err    error
}

type socketStatus int

const (
	SSActive socketStatus = iota
	SSPendingClose
	SSClosed
)

type socketEntry struct {
	status      socketStatus
	socket      *Socket
	addr        string
	receiveChan chan []byte
}

// NewServer creates, initiates, and returns a new server. This function should
// NOT block. Instead, it should spawn one or more goroutines (to handle things
// like accepting incoming client connections, triggering epoch events at
// fixed intervals, synchronizing events using a for-select loop like you saw in
// project 0, etc.) and immediately return. It should return a non-nil error if
// there was an error resolving or listening on the specified port number.
func NewServer(port int, params *Params) (Server, error) {
	ctx, cancel := context.WithCancelCause(context.Background())
	ctxOp, cancelOp := context.WithCancelCause(ctx)
	sv := &server{
		params:            params,
		nextSocketId:      1,
		socketEntries:     make(map[int]*socketEntry),
		addrLookup:        make(map[string]int),
		eventChan:         make(chan *svEvent),
		readChan:          make(chan *svResult),
		socketReadResults: NewQueue[*svResult](),
		ctx:               ctx,
		cancel:            cancel,
		ctxOp:             ctxOp,
		cancelOp:          cancelOp,
	}
	err := sv.startListen(port)
	if err != nil {
		return nil, err
	}
	go sv.main()
	return sv, nil
}

func (s *server) Read() (int, []byte, error) {
	select {
	case result := <-s.readChan:
		return result.connID, result.data, result.err
	case <-s.ctxOp.Done():
		return -1, nil, s.ctxOp.Err()
	}
}

func (s *server) Write(connId int, payload []byte) error {
	outResult := make(chan *svResult, 1)
	ev := &svEvent{
		svEventType: SVETRequestWrite,
		connID:      connId,
		inResult: &svResult{
			connID: connId,
			data:   payload,
		},
		outResult: outResult,
	}
	eventChan := s.eventChan
	for {
		select {
		case eventChan <- ev:
			eventChan = nil
		case result := <-outResult:
			return result.err
		case <-s.ctxOp.Done():
			return s.ctxOp.Err()
		}
	}
}

func (s *server) CloseConn(connId int) error {
	outResult := make(chan *svResult, 1)
	ev := &svEvent{
		svEventType: SVETCloseConn,
		connID:      connId,
		outResult:   outResult,
	}
	eventChan := s.eventChan
	for {
		select {
		case eventChan <- ev:
			eventChan = nil
		case result := <-outResult:
			return result.err
		case <-s.ctxOp.Done():
			return s.ctxOp.Err()
		}
	}
}

func (s *server) Close() error {
	eventChan := s.eventChan
	event := &svEvent{svEventType: SVETClose}
	for {
		select {
		case eventChan <- event:
			eventChan = nil
		case <-s.ctx.Done():
			return s.ctx.Err()
		}
	}
}

func (s *server) main() {
	var readChan chan *svResult
	for {
		select {
		case <-s.ctx.Done():
			return
		case readChan <- s.nextReadResult():
			s.socketReadResults.Pop()
			if s.socketReadResults.Empty() {
				readChan = nil
			}
		case sve := <-s.eventChan:
			switch sve.svEventType {
			case SVETUDPReceive:
				addrStr := sve.addr.String()
				id, exist := s.addrLookup[addrStr]
				if exist {
					entry := s.socketEntries[id]
					if entry == nil {
						panic("Socket entry from lookup is unexpectedly nil")
					}
					entry.receiveChan <- sve.inResult.data
				} else {
					if s.isPendingClose {
						// Don't accept new conn if we're already closing
						continue
					}
					// Only unmarshal to check if this is a new connection
					message := &Message{}
					err := json.Unmarshal(sve.inResult.data, message)
					if err != nil || message.Type != MsgConnect {
						continue
					}
					newId := s.nextSocketId
					s.nextSocketId++
					newReceiveChan := make(chan []byte, 1)
					entry := &socketEntry{
						receiveChan: newReceiveChan,
						status:      SSActive,
						socket: NewSocket(SocketParams{
							prefix:      "server",
							params:      s.params,
							sn:          &message.SeqNum,
							conn:        *s.conn,
							connID:      newId,
							receiveChan: newReceiveChan,
							write: func(_ lspnet.UDPConn, b []byte) (int, error) {
								return s.conn.WriteToUDP(b, sve.addr)
							},
						}),
					}
					s.socketEntries[newId] = entry
					s.addrLookup[addrStr] = newId
					entry.socket.Accept(message.SeqNum)
					go s.readSocket(entry.socket, newId)
				}
			case SVETRequestWrite:
				if s.isPendingClose {
					sve.outResult <- &svResult{
						err: errors.New("SVETRequestWrite: server pending close"),
					}
					break
				}
				entry, exist := s.socketEntries[sve.connID]
				if !exist {
					sve.outResult <- &svResult{
						err: errors.New("SVETRequestWrite: connID does not exist"),
					}
					break
				}
				if entry.status != SSActive {
					sve.outResult <- &svResult{
						err: errors.New("SVETRequestWrite: conn already closed or pending close"),
					}
					break
				}
				err := entry.socket.Write(sve.inResult.data)
				sve.outResult <- &svResult{
					err: err,
				}
			case SVETSocketRead:
				s.socketReadResults.Push(sve.inResult)
				readChan = s.readChan
				if sve.inResult.err == nil {
					break
				}
				// Handle error by marking socket as closed
				entry, exist := s.socketEntries[sve.connID]
				if !exist {
					break
				}
				entry.status = SSClosed
				if s.checkAllClosed() {
					return
				}
			case SVETCloseConn:
				entry, exist := s.socketEntries[sve.connID]
				if !exist {
					sve.outResult <- &svResult{
						err: errors.New("SVETCloseConn: connID does not exist"),
					}
					break
				}
				if entry.status == SSActive {
					entry.status = SSPendingClose
					// Close and forget; don't need to wait
					go func() {
						_ = entry.socket.Close()
					}()
				}
				sve.outResult <- &svResult{}
			case SVETClose:
				s.cancelOp(errors.New("server called closed"))
				s.isPendingClose = true
				for _, entry := range s.socketEntries {
					if entry.status == SSActive {
						entry.status = SSPendingClose
						go func() {
							_ = entry.socket.Close()
						}()
					}
				}
				if s.checkAllClosed() {
					return
				}
			}
		}
	}
}

func (s *server) startListen(port int) error {
	svAddr, err := lspnet.ResolveUDPAddr("udp", fmt.Sprintf(":%d", port))
	if err != nil {
		return err
	}
	conn, err := lspnet.ListenUDP("udp", svAddr)
	if err != nil {
		return err
	}
	s.conn = conn
	b := make([]byte, DefaultUdpReadBufferSize)
	go func() {
		for {
			n, addr, err := conn.ReadFromUDP(b)
			c := make([]byte, n)
			copy(c, b[:n])
			if err != nil {
				return
			}
			s.eventChan <- &svEvent{
				svEventType: SVETUDPReceive,
				addr:        addr,
				inResult: &svResult{
					data: c,
				},
			}
		}
	}()
	return nil
}

func (s *server) readSocket(socket *Socket, connID int) {
	for {
		b, err := socket.Read()
		s.eventChan <- &svEvent{
			svEventType: SVETSocketRead,
			connID:      connID,
			inResult: &svResult{
				connID: connID,
				data:   b,
				err:    err,
			},
		}
		if err != nil {
			return
		}
	}
}

func (s *server) nextReadResult() *svResult {
	if s.socketReadResults.Empty() {
		return nil
	}
	return s.socketReadResults.Front()
}

func (s *server) checkAllClosed() bool {
	if !s.isPendingClose {
		return false
	}
	for _, entry := range s.socketEntries {
		if entry.status != SSClosed {
			return false
		}
	}
	s.cancel(nil)
	s.conn.Close()
	return true
}
