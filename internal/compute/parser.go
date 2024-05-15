package compute

import (
	"context"
	"fmt"
	"go.uber.org/zap"
)

type (
	parser struct {
		logger       *zap.Logger
		stateMachine *stateMachine
	}
)

func NewParser(log *zap.Logger) *parser {
	return &parser{
		stateMachine: newStateMachine(),
		logger:       log,
	}
}

func (c *parser) Parse(_ context.Context, query string) ([]Token, error) {
	for i := range query {
		sym := query[i]

		state := ErrorState
		switch {
		case isValidLetter(sym):
			state = LetterState
		case isValidSpace(sym):
			state = SpaceState
		default:
			return nil, fmt.Errorf("wrong symbol '%s' at pos %d", string(sym), i)
		}

		c.stateMachine.process(state, sym)
	}

	return c.stateMachine.Tokens(), nil
}

func isValidLetter(symbol byte) bool {
	return (symbol >= 'a' && symbol <= 'z') ||
		(symbol >= 'A' && symbol <= 'Z') ||
		(symbol >= '0' && symbol <= '9') ||
		symbol == '_' ||
		symbol == '*' ||
		symbol == '/'
}

func isValidSpace(symbol byte) bool {
	return symbol == ' ' || symbol == '\t' || symbol == '\n'
}
