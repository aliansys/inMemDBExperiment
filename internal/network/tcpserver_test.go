package network

import (
	"context"
	"go.uber.org/zap"
	"net"
	"testing"
)

func TestTCPServer_Serve(t *testing.T) {
	tests := []struct {
		name    string
		wantErr bool
		wantMsg []byte
	}{
		{
			name:    "success",
			wantErr: false,
			wantMsg: []byte("hello world"),
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s, err := NewTCPServer(":55443", 10, zap.NewNop())
			if (err != nil) != tt.wantErr {
				t.Errorf("Serve() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if err != nil {
				return
			}

			err = s.Listen()
			if (err != nil) != tt.wantErr {
				t.Errorf("Listen() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if err != nil {
				return
			}

			defer s.Close()

			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()

			go func() {
				err = s.Serve(ctx, func(ctx context.Context, msg []byte) ([]byte, error) {
					if string(msg) != string(tt.wantMsg) {
						t.Errorf("Serve() got = %s, want %s", msg, tt.wantMsg)
					}

					return msg, nil
				})

				if (err != nil) != tt.wantErr {
					t.Errorf("Serve() error = %v, wantErr %v", err, tt.wantErr)
				}
			}()

			c, err := net.Dial("tcp", ":55443")
			if err != nil {
				t.Errorf("net.Dial() error = %v, wantErr %v", err, tt.wantErr)
				return
			}

			err = writeMessage(c, tt.wantMsg)
			if err != nil {
				t.Errorf("client.Close() error = %v, wantErr %v", err, tt.wantErr)
				return
			}

			err = c.Close()
			if err != nil {
				t.Errorf("client.Close() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
		})
	}
}
