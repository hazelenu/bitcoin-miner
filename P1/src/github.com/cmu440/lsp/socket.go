package lsp

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/cmu440/lspnet"
	log2 "log"
	"math"
	"math/rand"
	"time"
)

var enableLog = false

type socketMode int

const (
	SocketModeUnspecified socketMode = iota
	SocketModeListener
	SocketModeDialerPending
	SocketModeDialer
)

type socketEventType int

const (
	SocketEventConnect socketEventType = iota
	SocketEventAccept
	SocketEventSend
	SocketEventReceive
	SocketEventClose
)

type socketEvent struct {
	eventType socketEventType
	payload   []byte
	remoteSn  int
	signal    chan connectResult
}

type connectResult struct {
	connID int
	err    error
}

type SocketWriteFunc func(conn lspnet.UDPConn, b []byte) (int, error)

type SocketParams struct {
	prefix      string
	params      *Params
	conn        lspnet.UDPConn
	write       SocketWriteFunc
	connID      int
	receiveChan chan []byte // A channel that receives bytes from remote. If absent, a default implementation that reads from conn is used.
	sn          *int
}

type Socket struct {
	prefix        string // For debug
	connID        int
	params        *Params
	conn          lspnet.UDPConn
	socketMode    socketMode
	connectedChan chan connectResult
	write         SocketWriteFunc
	receiveChan   chan []byte // Receive bytes from remote
	deliverChan   chan []byte // Signal delivery to Read() calls; send a queue pointer to avoid Top() call when it may be empty
	eventChan     chan socketEvent
	epoch         int                      // Current epoch timestamp
	epochSent     bool                     // Has sent anything in this epoch
	lastHeartbeat int                      // Epoch timestamp in which the last heartbeat from remote is received
	sn            int                      // Local seq num
	nextDeliverSn int                      // Next remote seq num to be delivered
	deliveryMp    map[int]bool             // Lookup for SN currently in the PQ
	deliveryPq    *PriorityQueue[*Message] // Priority queue for pending deliveries
	slidingWindow *SlidingWindow
	readQ         *Queue[[]byte] // Buffer of data for consumers to read from
	writeQ        *Queue[[]byte] // Buffer of data waiting to be sent and put in outMsgWindow
	pendingClose  bool
	context       context.Context // Context to propagate cancellation (connection lost)
	cancel        context.CancelCauseFunc
}

type SocketError struct {
	message string
}

func (e *SocketError) Error() string {
	return e.message
}

func NewSocket(socketParams SocketParams) *Socket {
	if socketParams.params == nil {
		socketParams.params = NewParams()
	}
	newContext, cancel := context.WithCancelCause(context.Background())
	if socketParams.write == nil {
		panic("Write function is required in params")
	}
	var sn int
	if socketParams.sn != nil {
		sn = *socketParams.sn
	} else {
		sn = nonce()
	}
	s := &Socket{
		prefix:        socketParams.prefix,
		connID:        socketParams.connID,
		params:        socketParams.params,
		conn:          socketParams.conn,
		socketMode:    SocketModeUnspecified,
		write:         socketParams.write,
		receiveChan:   socketParams.receiveChan,
		deliverChan:   make(chan []byte, 1),
		eventChan:     make(chan socketEvent),
		epoch:         0,
		epochSent:     false,
		lastHeartbeat: 0,
		sn:            *socketParams.sn,
		nextDeliverSn: *socketParams.sn,
		deliveryMp:    make(map[int]bool),
		deliveryPq: NewPriorityQueue[*Message](func(a *Message, b *Message) bool {
			return a.SeqNum < b.SeqNum
		}),
		slidingWindow: NewSlidingWindow(&SlidingWindowParams{
			params: socketParams.params,
			sn:     sn,
		}),
		pendingClose: false,
		readQ:        NewQueue[[]byte](),
		writeQ:       NewQueue[[]byte](),
		context:      newContext,
		cancel:       cancel,
	}
	if s.receiveChan == nil {
		s.receiveChan = s.newReceiver(newContext, socketParams.conn)
	}
	go s.receive()
	go s.main()
	return s
}

func (s *Socket) newReceiver(context context.Context, conn lspnet.UDPConn) chan []byte {
	ch := make(chan []byte, 1)
	go func() {
		b := make([]byte, DefaultUdpReadBufferSize)
		for {
			select {
			case <-context.Done():
				return
			default:
				n, _, err := conn.ReadFromUDP(b)
				if err != nil {
					s.log("error on read", err)
					continue
				}
				c := make([]byte, n)
				copy(c, b[:n])
				ch <- c
			}
		}
	}()
	return ch
}

func (s *Socket) receive() {
	for {
		select {
		case data := <-s.receiveChan:
			s.eventChan <- socketEvent{
				eventType: SocketEventReceive,
				payload:   data,
			}
		case <-s.context.Done():
			return
		}
	}
}

func (s *Socket) main() {
	ticker := time.NewTicker(time.Duration(s.params.EpochMillis) * time.Millisecond)
	defer ticker.Stop()
	epochTick := ticker.C
	var deliverChan chan []byte
	for {
		select {
		case <-s.context.Done():
			return
		case <-epochTick:
			s.epoch++
			if s.epoch-s.lastHeartbeat >= s.params.EpochLimit {
				s.log("connection timed out", s.epoch, s.lastHeartbeat, s.params.EpochLimit)
				s.handleConnectionLost()
				break
			}
			resends, err := s.slidingWindow.GetMessagesToResend(s.epoch)
			s.log("messages to resend:", len(resends), err)
			if err != nil {
				switch err.errorType {
				case SWEConnectionLost:
					s.handleConnectionLost()
				}
				continue
			}
			for _, resend := range resends {
				s.writeMessage(resend)
			}
			if !s.epochSent && len(resends) == 0 {
				s.heartbeat()
			}
			s.epochSent = false
		case deliverChan <- s.nextInReadQ():
			s.readQ.Pop()
			if s.readQ.Empty() {
				deliverChan = nil
			}
		case event := <-s.eventChan:
			switch event.eventType {
			case SocketEventConnect:
				if s.socketMode != SocketModeUnspecified {
					event.signal <- connectResult{
						err: errors.New("socket not in init state"),
					}
					break
				}
				s.socketMode = SocketModeDialerPending
				s.connectedChan = event.signal
				s.sendConnect()
			case SocketEventAccept:
				s.socketMode = SocketModeListener
				s.sn++
				s.nextDeliverSn = event.remoteSn + 1
				s.slidingWindow.nextAckSn = s.sn
				s.ack(event.remoteSn)
			case SocketEventSend:
				if !s.sendData(event.payload) {
					s.writeQ.Push(event.payload)
				}
			case SocketEventReceive:
				msg := &Message{}
				err := json.Unmarshal(event.payload, msg)
				if err != nil {
					panic(err)
				}
				s.log("Received msg", msg.String())
				switch msg.Type {
				case MsgConnect:
					// Handle client connect msg resend
					s.ack(msg.SeqNum)
				case MsgData:
					s.lastHeartbeat = s.epoch
					if !s.verifyMessage(msg) {
						break
					}
					s.ack(msg.SeqNum)
					if msg.SeqNum < s.nextDeliverSn || s.deliveryMp[msg.SeqNum] {
						break
					}
					s.deliveryMp[msg.SeqNum] = true
					s.deliveryPq.Push(msg)
					for s.deliveryPq.Len() > 0 && s.deliveryPq.Top().SeqNum == s.nextDeliverSn {
						s.readQ.Push(s.deliveryPq.Pop().Payload)
						delete(s.deliveryMp, s.nextDeliverSn)
						s.nextDeliverSn++
					}
					if !s.readQ.Empty() {
						deliverChan = s.deliverChan
					}
				case MsgAck, MsgCAck:
					s.handleAck(msg)
				}
			case SocketEventClose:
				s.log("called close")
				s.pendingClose = true
				s.checkPendingClose()
			}
		}
	}
}

func (s *Socket) verifyMessage(msg *Message) bool {
	payloadLen := len(msg.Payload)
	if payloadLen < msg.Size {
		s.log("payload size shorter than specified size", payloadLen, msg.Size)
		return false
	} else if payloadLen > msg.Size {
		msg.Payload = msg.Payload[:msg.Size]
		s.log(fmt.Sprintf("payload size longer than specified size, truncated from %d to %d", payloadLen, len(msg.Payload)))
	}
	answer := CalculateChecksum(msg.ConnID, msg.SeqNum, msg.Size, msg.Payload)
	if msg.Checksum != answer {
		s.log("checksum mismatch", msg.Checksum, answer)
	}
	return msg.Checksum == answer
}

func (s *Socket) handleAck(msg *Message) {
	var fn func(sn int)
	switch msg.Type {
	case MsgAck:
		fn = s.slidingWindow.Ack
	case MsgCAck:
		fn = s.slidingWindow.CAck
	default:
		panic("handle ack received non ack message")
	}

	s.lastHeartbeat = s.epoch + 1

	if s.socketMode == SocketModeDialerPending {
		s.connID = msg.ConnID
		s.connectedChan <- connectResult{
			connID: s.connID,
			err:    nil,
		}
		s.connectedChan = nil
		s.socketMode = SocketModeDialer
		s.nextDeliverSn = msg.SeqNum + 1
	} else if msg.SeqNum == 0 {
		// Heartbeat
		return
	}
	fn(msg.SeqNum)
	for !s.writeQ.Empty() && s.sendData(s.writeQ.Front()) {
		s.writeQ.Pop()
	}
	s.checkPendingClose()
}

func (s *Socket) nextInReadQ() []byte {
	if s.readQ.Empty() {
		return nil
	}
	return s.readQ.Front()
}

/*
*
sendData

Returns true if data message has been sent; false otherwise (mainly due to ACK window full)
*/
func (s *Socket) sendData(payload []byte) bool {
	if !s.slidingWindow.CanPlace(s.sn) {
		return false
	}
	checksum := CalculateChecksum(s.connID, s.sn, len(payload), payload)
	msg := NewData(s.connID, s.sn, len(payload), payload, checksum)
	s.sendMessageUnsafe(msg)
	return true
}

func (s *Socket) sendConnect() {
	if !s.slidingWindow.CanPlace(s.sn) {
		panic("Sliding window is full when trying to place connect message; shouldn't happen")
	}
	msg := NewConnect(s.sn)
	s.sendMessageUnsafe(msg)
}

func (s *Socket) sendMessageUnsafe(msg *Message) {
	s.writeMessage(msg)
	// UNSAFE: sliding window may be full and panic; caller should check CanPlace
	s.slidingWindow.Place(msg, s.epoch)
	s.sn++
	s.epochSent = true
}

func (s *Socket) ack(sn int) {
	s.writeMessage(NewAck(s.connID, sn))
}

func (s *Socket) heartbeat() {
	s.ack(0)
}

func (s *Socket) writeMessage(msg *Message) {
	b, err := json.Marshal(msg)
	if err != nil {
		panic(err)
	}
	_, err = s.write(s.conn, b)
	if err != nil {
		panic(err)
	}
	s.log("Written msg", msg.String())
}

func nonce() int {
	// Prevent really unlucky overflow even though writeup asks us not to worry
	return rand.Intn(math.MaxInt / 2)
}

func (s *Socket) Write(b []byte) error {
	c := make([]byte, len(b))
	copy(c, b)
	writeEvent := socketEvent{
		eventType: SocketEventSend,
		payload:   c,
	}
	select {
	// Note: Forfeit is intended if close is called and finalized before this write can be queued
	case s.eventChan <- writeEvent:
		return nil
	case <-s.context.Done():
		return s.context.Err()
	}
}

func (s *Socket) Read() ([]byte, error) {
	select {
	case data := <-s.deliverChan:
		return data, nil
	case <-s.context.Done():
		s.log("error on read:", s.context.Err())
		return nil, s.context.Err()
	}
}

func (s *Socket) Close() error {
	eventChan := s.eventChan
	closeEvent := socketEvent{
		eventType: SocketEventClose,
	}
	for {
		select {
		case eventChan <- closeEvent:
			eventChan = nil
		case <-s.context.Done():
			return s.context.Err()
		}
	}
}

func (s *Socket) Connect() (int, error) {
	signal := make(chan connectResult, 1)
	s.eventChan <- socketEvent{
		eventType: SocketEventConnect,
		signal:    signal,
	}
	select {
	case <-s.context.Done():
		return -1, s.context.Err()
	case result := <-signal:
		if result.err != nil {
			return -1, result.err
		} else {
			return result.connID, nil
		}
	}
}

func (s *Socket) Accept(remoteSn int) {
	s.eventChan <- socketEvent{
		eventType: SocketEventAccept,
		remoteSn:  remoteSn,
	}
}

func (s *Socket) checkPendingClose() bool {
	pendingWrite := s.slidingWindow.len > 0 || s.writeQ.Len() > 0
	if pendingWrite {
		return false
	}
	if !s.pendingClose {
		return false
	}
	s.cancel(nil)
	s.log("close finalized")
	return true
}

func (s *Socket) handleConnectionLost() {
	s.log("handle connection lost")
	s.cancel(&SocketError{message: "Connection lost"})
}

func (s *Socket) log(a ...any) {
	if !enableLog {
		return
	}

	var color string
	if s.prefix == "server" {
		color = "\u001B[31m"
	} else {
		color = "\u001B[33m"
	}
	prefix := fmt.Sprintf("%s%s %d [%d]: \u001B[0m", color, s.prefix, s.connID, s.epoch)
	content := fmt.Sprintln(a...)
	log2.Default().Printf("%s%s", prefix, content)
}
