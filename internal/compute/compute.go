package compute

import (
	"context"
	"errors"
	"go.uber.org/zap"
)

type (
	Token struct {
		Type  TokenType
		Value string
	}

	Query struct {
		Command CommandID
		Key     string
		Arg     string
	}

	TokenType int
	StateType int
	CommandID int

	Parser interface {
		Parse(context.Context, string) ([]Token, error)
	}

	Analyzer interface {
		Analyze(context.Context, []Token) (Query, error)
	}

	compute struct {
		logger   *zap.Logger
		parser   Parser
		analyzer Analyzer
	}
)

var (
	ErrEmptyQuery             = errors.New("empty query")
	ErrUnknownCommand         = errors.New("unknown command")
	ErrMustBeCommand          = errors.New("command must be a command")
	ErrWrongNumberOfArguments = errors.New("wrong number of arguments")
	ErrWrongArgument          = errors.New("wrong argument")
	ErrWrongKey               = errors.New("wrong key")
)

const (
	CommandType TokenType = iota + 1
	KeyType     TokenType = iota + 1
	ArgumentType
)

const (
	InitialState StateType = iota + 1
	LetterState
	SpaceState
	ErrorState
)

const (
	CommandGet CommandID = iota + 1
	CommandSet
	CommandDel
)

func New(parser Parser, analyzer Analyzer, logger *zap.Logger) *compute {
	return &compute{
		logger:   logger,
		parser:   parser,
		analyzer: analyzer,
	}
}

func (c *compute) HandleParse(ctx context.Context, q string) (Query, error) {
	tokens, err := c.parser.Parse(ctx, q)
	if err != nil {
		return Query{}, err
	}

	query, err := c.analyzer.Analyze(ctx, tokens)
	if err != nil {
		return Query{}, err
	}

	return query, nil
}
