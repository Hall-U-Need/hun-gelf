package gelf

import (
	"crypto/tls"
	"fmt"
	"net"
	"os"
	"time"

	lock "github.com/viney-shih/go-lock"
)

type TLSWriter struct {
	TCPWriter
	TlsConfig *tls.Config
}

func (w *TLSWriter) Dial(addr string) (net.Conn, error) {
	return tls.Dial("tcp", addr, w.TlsConfig)
}

func NewTLSWriter(addr string, tlsConfig *tls.Config) (*TLSWriter, error) {
	w := new(TLSWriter)
	w.MaxReconnect = DefaultMaxReconnect
	w.ReconnectDelay = DefaultReconnectDelay
	w.proto = "tls"
	w.addr = addr
	w.TlsConfig = tlsConfig
	w.mu = lock.NewChanMutex()

	var err error
	if w.conn, err = w.Dial(w.addr); err != nil {
		return nil, err
	}
	if w.hostname, err = os.Hostname(); err != nil {
		return nil, err
	}

	return w, nil
}

// WriteMessage sends the specified message to the GELF server
// specified in the call to New().  It assumes all the fields are
// filled out appropriately.  In general, clients will want to use
// Write, rather than WriteMessage.
func (w *TLSWriter) WriteMessage(m *Message) (err error) {
	buf := newBuffer()
	defer bufPool.Put(buf)
	messageBytes, err := m.toBytes(buf)
	if err != nil {
		return err
	}

	messageBytes = append(messageBytes, 0)

	n, err := w.writeToSocketWithReconnectAttempts(messageBytes)
	if err != nil {
		return err
	}
	if n != len(messageBytes) {
		return fmt.Errorf("bad write (%d/%d)", n, len(messageBytes))
	}

	return nil
}

func (w *TLSWriter) Write(p []byte) (n int, err error) {
	file, line := getCallerIgnoringLogMulti(1)

	m := constructMessage(p, w.hostname, w.Facility, file, line)

	if err = w.WriteMessage(m); err != nil {
		if !w.MessageBuffer[m] {
			w.MessageBuffer[m] = true
		}
		return 0, err
	}

	return len(p), nil
}

func (w *TLSWriter) writeToSocketWithReconnectAttempts(zBytes []byte) (n int, err error) {
	var errConn error
	var i int

	w.mu.Lock()
	for i = 0; i <= w.MaxReconnect; i++ {
		errConn = nil

		if w.conn != nil {
			n, err = w.conn.Write(zBytes)
			if err != nil {
				fmt.Println("Gelf write error", err.Error())
			}
		} else {
			err = fmt.Errorf("Connection was nil, will attempt reconnect")
		}
		if err != nil {
			time.Sleep(w.ReconnectDelay * time.Second)
			w.conn, errConn = w.Dial(w.addr)
			if errConn != nil {
				fmt.Println(errConn.Error())
			}
		} else {
			break
		}
	}
	w.mu.Unlock()

	if i > w.MaxReconnect {
		return 0, fmt.Errorf("Maximum reconnection attempts was reached; giving up")
	}
	if errConn != nil {
		return 0, fmt.Errorf("Write Failed: %s\nReconnection failed: %s", err, errConn)
	} else {
		for m := range w.MessageBuffer {
			fmt.Println("Flushing buffered messages", m)
			err := w.WriteMessage(m)
			if err == nil {
				fmt.Println("removing from buffer", m)
				delete(w.MessageBuffer, m)
			}
		}
	}
	return n, nil
}

// Close connection and interrupt blocked Read or Write operations
func (w *TLSWriter) Close() error {
	if w.conn == nil {
		return nil
	}
	// Wait a little that lasts logs messages enter in the log pipeline
	time.Sleep(time.Second)
	// Wait a little with the muttex for remaining transmission to finish
	w.mu.TryLockWithTimeout(time.Second)
	return w.conn.Close()
}
