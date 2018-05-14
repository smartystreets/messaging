package tcp

import (
	"log"
	"net"
)

func BindSocket(bindAddress string) (*net.TCPListener, error) {
	if address, err := net.ResolveTCPAddr("tcp", bindAddress); err != nil {
		log.Println("[WARN] TCP socket bind failure:", err)
		return nil, err
	} else if socket, err := net.ListenTCP("tcp", address); err != nil {
		log.Println("[WARN] TCP socket bind failure:", err)
		return nil, err
	} else {
		log.Printf("[INFO] Listening for TCP packets on %s.\n", address)
		return socket, nil
	}
}
