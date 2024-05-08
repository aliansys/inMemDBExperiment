package compute

import (
	"context"
	"fmt"
	"go.uber.org/zap"
)

type (
	analyzer struct {
		logger *zap.Logger
	}
)

func NewAnalyzer(logger *zap.Logger) *analyzer {
	return &analyzer{
		logger: logger,
	}
}

func (a *analyzer) Analyze(_ context.Context, tokens []Token) (Query, error) {
	if len(tokens) == 0 {
		return Query{}, ErrEmptyQuery
	}

	cmd := tokens[0]

	if cmd.Type != CommandType {
		return Query{}, ErrMustBeCommand
	}

	var q Query
	switch cmd.Value {
	case "GET":
		if len(tokens) != 2 {
			return Query{}, fmt.Errorf("%w: must be one", ErrWrongNumberOfArguments)
		}

		if len(tokens[1].Value) == 0 {
			return Query{}, ErrWrongArgument
		}

		q.Command = CommandGet
	case "SET":
		if len(tokens) != 3 {
			return Query{}, fmt.Errorf("%w: must be one", ErrWrongNumberOfArguments)
		}

		if tokens[1].Type != KeyType || len(tokens[1].Value) == 0 {
			return Query{}, fmt.Errorf("%w: key", ErrWrongKey)
		}

		if tokens[2].Type != ArgumentType || len(tokens[2].Value) == 0 {
			return Query{}, fmt.Errorf("%w: second", ErrWrongArgument)
		}

		q.Command = CommandSet
		q.Arg = tokens[2].Value
	case "DEL":
		if len(tokens) != 2 {
			return Query{}, fmt.Errorf("%w: must be one", ErrWrongNumberOfArguments)
		}

		if tokens[1].Type != KeyType || len(tokens[1].Value) == 0 {
			return Query{}, ErrWrongArgument
		}

		q.Command = CommandDel
	default:
		return Query{}, fmt.Errorf("%w: %s", ErrUnknownCommand, cmd.Value)
	}

	q.Key = tokens[1].Value

	return q, nil
}
