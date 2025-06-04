// Contains the implementation of a LSP client.

package lsp

import (
	"github.com/cmu440/lspnet"
)

type client struct {
	socket *Socket
	conn   *lspnet.UDPConn
	connID int
}

// NewClient creates, initiates, and returns a new client. This function
// should return after a connection with the server has been established
// (i.e., the client has received an Ack message from the server in response
// to its connection request), and should return a non-nil error if a
// connection could not be made (i.e., if after K epochs, the client still
// hasn't received an Ack message from the server in response to its K
// connection requests).
//
// initialSeqNum is an int representing the Initial Sequence Number (ISN) this
// client must use. You may assume that sequence numbers do not wrap around.
//
// hostport is a colon-separated string identifying the server's host address
// and port number (i.e., "localhost:9999").
func NewClient(hostport string, initialSeqNum int, params *Params) (Client, error) {
	addr, err := lspnet.ResolveUDPAddr("udp", hostport)
	if err != nil {
		return nil, err
	}
	conn, err := lspnet.DialUDP("udp", nil, addr)
	if err != nil {
		return nil, err
	}
	newSocket := NewSocket(SocketParams{
		prefix: "client",
		params: params,
		conn:   *conn,
		write: func(conn lspnet.UDPConn, b []byte) (int, error) {
			return conn.Write(b)
		},
		sn: &initialSeqNum,
	})
	connID, err := newSocket.Connect()
	if err != nil {
		_ = newSocket.Close()
		return nil, err
	}
	return &client{socket: newSocket, connID: connID, conn: conn}, nil
}

func (c *client) ConnID() int {
	return c.connID
}

func (c *client) Read() ([]byte, error) {
	return c.socket.Read()
}

func (c *client) Write(payload []byte) error {
	return c.socket.Write(payload)
}

func (c *client) Close() error {
	if c.conn != nil {
		defer c.conn.Close()
	}
	return c.socket.Close()
}
