package compute

import (
	"context"
	"go.uber.org/zap"
	"reflect"
	"testing"
)

func Test_analyzer_Analyze(t *testing.T) {
	type args struct {
		ctx    context.Context
		tokens []Token
	}

	tests := []struct {
		name    string
		args    args
		want    Query
		wantErr bool
	}{
		{
			name: "No tokens passed",
			args: args{
				ctx:    context.Background(),
				tokens: []Token{},
			},
			want:    Query{},
			wantErr: true,
		},
		{
			name: "Starts with wrong token type",
			args: args{
				ctx:    context.Background(),
				tokens: []Token{{Type: ArgumentType, Value: "SET"}},
			},
			want:    Query{},
			wantErr: true,
		},
		{
			name: "Unknown command",
			args: args{
				ctx:    context.Background(),
				tokens: []Token{{Type: CommandType, Value: "set"}},
			},
			want:    Query{},
			wantErr: true,
		},

		// GET command tests
		{
			name: "GET. Wrong number of args - zero",
			args: args{
				ctx:    context.Background(),
				tokens: []Token{{Type: CommandType, Value: "GET"}},
			},
			want:    Query{},
			wantErr: true,
		},
		{
			name: "GET. Wrong number of args - two",
			args: args{
				ctx:    context.Background(),
				tokens: []Token{{Type: CommandType, Value: "GET"}, {Type: KeyType, Value: "key"}, {Type: ArgumentType, Value: "arg"}},
			},
			want:    Query{},
			wantErr: true,
		},
		{
			name: "GET. Wrong argument - empty string",
			args: args{
				ctx:    context.Background(),
				tokens: []Token{{Type: CommandType, Value: "GET"}, {Type: KeyType, Value: ""}},
			},
			want:    Query{},
			wantErr: true,
		},
		{
			name: "GET. Valid query",
			args: args{
				ctx:    context.Background(),
				tokens: []Token{{Type: CommandType, Value: "GET"}, {Type: KeyType, Value: "key"}},
			},
			want: Query{
				Command: CommandGet,
				Key:     "key",
			},
			wantErr: false,
		},

		// SET command tests
		{
			name: "SET. Wrong number of args - zero",
			args: args{
				ctx:    context.Background(),
				tokens: []Token{{Type: CommandType, Value: "SET"}},
			},
			want:    Query{},
			wantErr: true,
		},
		{
			name: "SET. Wrong number of args - one",
			args: args{
				ctx:    context.Background(),
				tokens: []Token{{Type: CommandType, Value: "SET"}, {Type: KeyType, Value: "key"}},
			},
			want:    Query{},
			wantErr: true,
		},
		{
			name: "SET. Wrong Key - empty string",
			args: args{
				ctx:    context.Background(),
				tokens: []Token{{Type: CommandType, Value: "SET"}, {Type: KeyType, Value: ""}, {Type: ArgumentType, Value: "val"}},
			},
			want:    Query{},
			wantErr: true,
		},
		{
			name: "SET. Wrong Argument - empty string",
			args: args{
				ctx:    context.Background(),
				tokens: []Token{{Type: CommandType, Value: "SET"}, {Type: KeyType, Value: "key"}, {Type: ArgumentType, Value: ""}},
			},
			want:    Query{},
			wantErr: true,
		},
		{
			name: "SET. Valid query",
			args: args{
				ctx:    context.Background(),
				tokens: []Token{{Type: CommandType, Value: "SET"}, {Type: KeyType, Value: "key"}, {Type: ArgumentType, Value: "arg"}},
			},
			want: Query{
				Command: CommandSet,
				Key:     "key",
				Arg:     "arg",
			},
			wantErr: false,
		},

		// DEL command tests
		{
			name: "DEL. Wrong number of args - zero",
			args: args{
				ctx:    context.Background(),
				tokens: []Token{{Type: CommandType, Value: "DEL"}},
			},
			want:    Query{},
			wantErr: true,
		},
		{
			name: "DEL. Wrong number of args - two",
			args: args{
				ctx:    context.Background(),
				tokens: []Token{{Type: CommandType, Value: "DEL"}, {Type: KeyType, Value: "key"}, {Type: ArgumentType, Value: "arg"}},
			},
			want:    Query{},
			wantErr: true,
		},
		{
			name: "DEL. Wrong argument - empty string",
			args: args{
				ctx:    context.Background(),
				tokens: []Token{{Type: CommandType, Value: "DEL"}, {Type: KeyType, Value: ""}},
			},
			want:    Query{},
			wantErr: true,
		},
		{
			name: "DEL. Valid query",
			args: args{
				ctx:    context.Background(),
				tokens: []Token{{Type: CommandType, Value: "DEL"}, {Type: KeyType, Value: "key"}},
			},
			want: Query{
				Command: CommandDel,
				Key:     "key",
			},
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			a := NewAnalyzer(zap.NewNop())
			got, err := a.Analyze(tt.args.ctx, tt.args.tokens)
			if (err != nil) != tt.wantErr {
				t.Errorf("Analyze() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("Analyze() got = %v, want %v", got, tt.want)
			}
		})
	}
}
