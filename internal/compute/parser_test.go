package compute

import (
	"context"
	"go.uber.org/zap"
	"reflect"
	"testing"
)

func Test_compute_Parse(t *testing.T) {
	tests := []struct {
		name    string
		query   string
		ctx     context.Context
		want    []Token
		wantErr bool
	}{
		{"Valid Letters. Command only", "abc", context.Background(), []Token{
			{Type: CommandType, Value: "abc"},
		}, false},
		{"Valid Letters. Extra spaces", "  abc\t", context.Background(), []Token{
			{Type: CommandType, Value: "abc"},
		}, false},
		{"Only spaces", "   ", context.Background(), []Token{}, false},
		{"Invalid Symbol. Cyrillic letters", "Яяййххъ", context.Background(), nil, true},
		{"Invalid Symbol. Unsupported sign", "a%", context.Background(), nil, true},
		{"Valid letters. Command + Key", "abc 123", context.Background(), []Token{
			{Type: CommandType, Value: "abc"},
			{Type: KeyType, Value: "123"},
		}, false},
		{"Valid letters. Command + Key + Argument", "abc 123 777", context.Background(), []Token{
			{Type: CommandType, Value: "abc"},
			{Type: KeyType, Value: "123"},
			{Type: ArgumentType, Value: "777"},
		}, false},
		{"Empty String", "", context.Background(), []Token{}, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			p := NewParser(zap.NewNop())
			got, err := p.Parse(tt.ctx, tt.query)
			if (err != nil) != tt.wantErr {
				t.Errorf("Parse() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("Parse() got = %v, want %v", got, tt.want)
			}
		})
	}
}
