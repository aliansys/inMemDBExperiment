package network

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"net"
)

const (
	headerSize = 2
)

func writeMessage(connection net.Conn, msg []byte) error {
	payloadLength := uint16(len(msg))
	buf := bytes.NewBuffer(make([]byte, 0, payloadLength+headerSize))
	err := binary.Write(buf, binary.BigEndian, payloadLength)
	if err != nil {
		return err
	}

	_, err = buf.Write(msg)
	if err != nil {
		return err
	}

	_, err = connection.Write(buf.Bytes())
	return err
}

func readMessage(connection net.Conn) ([]byte, error) {
	header := make([]byte, headerSize)
	_, err := connection.Read(header)
	if err != nil {
		return nil, fmt.Errorf("failed to read header: %w", err)
	}

	payloadResponseLength := binary.BigEndian.Uint16(header)
	payload := make([]byte, payloadResponseLength)
	_, err = connection.Read(payload)
	if err != nil {
		return nil, fmt.Errorf("failed to read payload: %w", err)
	}

	return payload, nil
}
