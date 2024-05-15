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

	switch cmd.Value {
	case "GET":
		return a.analyzeGET(tokens)
	case "SET":
		return a.analyzeSET(tokens)
	case "DEL":
		return a.analyzeDEL(tokens)
	default:
		return Query{}, fmt.Errorf("%w: %s", ErrUnknownCommand, cmd.Value)
	}
}

func (a *analyzer) analyzeGET(tokens []Token) (Query, error) {
	if len(tokens) != 2 {
		return Query{}, fmt.Errorf("%w: must be one", ErrWrongNumberOfArguments)
	}

	if len(tokens[1].Value) == 0 {
		return Query{}, ErrWrongArgument
	}

	return Query{
		Command: CommandGet,
		Key:     tokens[1].Value,
	}, nil
}

func (a *analyzer) analyzeSET(tokens []Token) (Query, error) {
	if len(tokens) != 3 {
		return Query{}, fmt.Errorf("%w: must be one", ErrWrongNumberOfArguments)
	}

	if tokens[1].Type != KeyType || len(tokens[1].Value) == 0 {
		return Query{}, fmt.Errorf("%w: key", ErrWrongKey)
	}

	if tokens[2].Type != ArgumentType || len(tokens[2].Value) == 0 {
		return Query{}, fmt.Errorf("%w: second", ErrWrongArgument)
	}

	return Query{
		Command: CommandSet,
		Key:     tokens[1].Value,
		Arg:     tokens[2].Value,
	}, nil
}

func (a *analyzer) analyzeDEL(tokens []Token) (Query, error) {
	if len(tokens) != 2 {
		return Query{}, fmt.Errorf("%w: must be one", ErrWrongNumberOfArguments)
	}

	if tokens[1].Type != KeyType || len(tokens[1].Value) == 0 {
		return Query{}, ErrWrongArgument
	}

	return Query{
		Command: CommandDel,
		Key:     tokens[1].Value,
	}, nil
}
