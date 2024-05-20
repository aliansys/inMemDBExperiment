package network

import (
	"net"
	"testing"
	"time"
)

func TestTCPClient_SendToConstantlyWorkingServer(t *testing.T) {
	address := ":55443"

	type args struct {
		msg []byte
	}
	tests := []struct {
		before  func()
		name    string
		args    args
		want    string
		wantErr bool
	}{
		{
			before: func() {},
			name:   "connection failed",
			args: args{
				msg: nil,
			},
			want:    "",
			wantErr: true,
		},
		{
			before: func() {
				runTestEchoServer(t, address)
			},
			name: "send empty. ok",
			args: args{
				msg: nil,
			},
			want:    "",
			wantErr: false,
		},
		{
			before: func() {
				runTestEchoServer(t, address)
			},
			name: "send data. ok",
			args: args{
				msg: []byte("hello world"),
			},
			want:    "hello world",
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if tt.before != nil {
				tt.before()
			}

			c, err := NewTCPClient(address, WithTimeout(time.Second))
			if (err != nil) != tt.wantErr {
				t.Errorf("NewTCPClient() error = %v, wantErr nil", err)
				return
			}
			if err != nil {
				return
			}
			defer c.Close()

			got, err := c.Send(tt.args.msg)
			if (err != nil) != tt.wantErr {
				t.Errorf("Send() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if got != tt.want {
				t.Errorf("Send() got = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestTCPClient_SendAndReconnect(t *testing.T) {
	address := ":55443"
	listener, err := net.Listen("tcp", address)
	if err != nil {
		t.Fatal("net.Listen:", err)
	}

	stop := make(chan struct{})
	stopped := make(chan struct{})

	go func() {
		connection, err := listener.Accept()
		if err != nil {
			return
		}

		<-stop
		err = connection.Close()
		if err != nil {
			t.Fatal("connection.Close:", err)
		}
		err = listener.Close()
		if err != nil {
			t.Fatal("listener.Close:", err)
		}

		stopped <- struct{}{}
		return
	}()

	c, err := NewTCPClient(address, WithReconnectMaxTries(1))
	if err != nil {
		t.Errorf("NewTCPClient() error = %v, wantErr nil", err)
		return
	}

	defer c.Close()

	stop <- struct{}{}
	<-stopped

	runTestEchoServer(t, address)

	want := "hello world"

	got, err := c.Send([]byte(want))
	if err != nil {
		t.Errorf("Send() error = %v, wantErr nil", err)
		return
	}

	if got != want {
		t.Errorf("Send() got = %v, want %v", got, want)
	}
}

func runTestEchoServer(t *testing.T, address string) {
	listener, err := net.Listen("tcp", address)
	if err != nil {
		t.Fatal("net.Listen:", err)
	}

	go func() {
		connection, err := listener.Accept()
		if err != nil {
			return
		}

		msg, err := readMessage(connection)
		if err != nil {
			t.Fatal("readMessage:", err)
		}

		err = writeMessage(connection, msg)
		if err != nil {
			t.Fatal("writeMessage:", err)
		}

		defer func() {
			err = connection.Close()
			if err != nil {
				t.Fatal("connection.Close:", err)
			}
			err = listener.Close()
			if err != nil {
				t.Fatal("listener.Close:", err)
			}
		}()
	}()
}
