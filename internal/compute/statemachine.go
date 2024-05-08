package compute

import "strings"

type (
	stateMachine struct {
		state       StateType
		tokens      []Token
		tokenBuffer Token
		wordBuffer  strings.Builder
	}
)

func (s *stateMachine) process(t StateType, sym byte, pos uint) {
	switch t {
	case LetterState:
		switch len(s.tokens) {
		case 0:
			s.tokenBuffer.Type = CommandType
		case 1:
			s.tokenBuffer.Type = KeyType
		default:
			s.tokenBuffer.Type = ArgumentType
		}

		s.wordBuffer.WriteByte(sym)
	case SpaceState:
		if s.state == LetterState {
			s.flushBuffer()
		}

		s.tokenBuffer = Token{}
	}

	s.state = t
}

func (s *stateMachine) Tokens() []Token {
	if s.tokenBuffer.Type != 0 {
		s.flushBuffer()
	}

	res := make([]Token, len(s.tokens))
	copy(res, s.tokens)

	s.tokens = make([]Token, 0, len(res))
	return res
}

func (s *stateMachine) flushBuffer() {
	s.tokenBuffer.Value = s.wordBuffer.String()
	s.wordBuffer.Reset()
	s.tokens = append(s.tokens, s.tokenBuffer)
}
