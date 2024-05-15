package compute

import "strings"

type (
	stateMachine struct {
		state       StateType
		tokens      []Token
		tokenBuffer Token
		wordBuffer  strings.Builder

		transitions map[StateType]map[StateType]transition
	}

	transition struct {
		apply func(sym byte)
	}
)

func newStateMachine() *stateMachine {
	s := &stateMachine{
		state:  InitialState,
		tokens: make([]Token, 0, 3),
	}

	s.transitions = map[StateType]map[StateType]transition{
		InitialState: {
			LetterState: {apply: s.applyLetter},
			SpaceState:  {apply: s.applySpace},
		},
		LetterState: {
			LetterState: {apply: s.applyLetter},
			SpaceState:  {apply: s.applySpace},
		},
		SpaceState: {
			LetterState: {apply: s.applyLetter},
			SpaceState:  {apply: s.applySpace},
		},
		ErrorState: {},
	}

	return s
}

func (s *stateMachine) process(t StateType, sym byte) {
	transition, _ := s.transitions[s.state][t]
	if transition.apply != nil {
		transition.apply(sym)
	}
}

func (s *stateMachine) applyLetter(sym byte) {
	switch len(s.tokens) {
	case 0:
		s.tokenBuffer.Type = CommandType
	case 1:
		s.tokenBuffer.Type = KeyType
	default:
		s.tokenBuffer.Type = ArgumentType
	}

	s.wordBuffer.WriteByte(sym)
	s.state = LetterState
}

func (s *stateMachine) applySpace(_ byte) {
	if s.state == LetterState {
		s.flushBuffer()
	}

	s.tokenBuffer = Token{}
	s.state = SpaceState
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
