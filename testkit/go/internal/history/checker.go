package history

import (
	"context"
	"errors"
	"fmt"
	"sort"

	"google.golang.org/grpc/codes"
)

// Verdict distinguishes a proven history from a rejected or unfinished search.
type Verdict string

const (
	Linearizable Verdict = "linearizable"
	Invalid      Verdict = "invalid"
	Inconclusive Verdict = "inconclusive"
)

// Model applies one operation to an immutable state. For successful calls it
// must compare every field it claims to model. For failed calls the model
// determines whether the reported error permits omission or a late effect.
// Unsupported semantics must return an error instead of certifying a history.
type Model interface {
	Apply(state any, operation Observation) (next any, matches bool, err error)
	CanOmit(state any, operation Observation) (bool, error)
}

// SearchResult includes the number of explored states for reproducibility.
type SearchResult struct {
	Verdict Verdict
	States  uint64
	Order   []uint64
}

// Check explores orders consistent with observed RPC intervals. Failed calls
// have unknown outcomes: they can be omitted or take effect after their return.
// Exhausting the state budget, cancellation, or unsupported model behavior is
// inconclusive rather than evidence of either correctness or a violation.
func Check(ctx context.Context, observations []Observation, model Model, initial any, maxStates uint64) (SearchResult, error) {
	if ctx == nil || model == nil || len(observations) == 0 || len(observations) > 20 || maxStates == 0 {
		return SearchResult{}, errors.New("invalid history search parameters")
	}
	if err := validateObservations(observations); err != nil {
		return SearchResult{}, err
	}
	search := checker{ctx: ctx, operations: observations, model: model, maxStates: maxStates}
	found := search.visit(initial, 0, nil)
	result := SearchResult{States: search.states}
	switch {
	case found:
		result.Verdict, result.Order = Linearizable, search.order
	case search.incomplete:
		result.Verdict = Inconclusive
	default:
		result.Verdict = Invalid
	}
	return result, nil
}

type checker struct {
	ctx        context.Context
	operations []Observation
	model      Model
	maxStates  uint64
	states     uint64
	incomplete bool
	order      []uint64
}

func (search *checker) visit(state any, done uint64, order []uint64) bool {
	if search.ctx.Err() != nil || search.states >= search.maxStates {
		search.incomplete = true
		return false
	}
	search.states++
	if done == (uint64(1)<<len(search.operations))-1 {
		search.order = append([]uint64(nil), order...)
		return true
	}
	for index, operation := range search.operations {
		bit := uint64(1) << index
		if done&bit != 0 || !search.eligible(index, done) {
			continue
		}
		if operation.Code != "OK" {
			mayOmit, err := search.model.CanOmit(state, operation)
			if err != nil {
				search.incomplete = true
				continue
			}
			if mayOmit && search.visit(state, done|bit, order) {
				return true
			}
		}
		next, matches, err := search.model.Apply(state, operation)
		if err != nil {
			search.incomplete = true
			continue
		}
		if matches && search.visit(next, done|bit, append(order, operation.ID)) {
			return true
		}
	}
	return false
}

func (search *checker) eligible(index int, done uint64) bool {
	candidate := search.operations[index]
	for previous, operation := range search.operations {
		if previous == index || done&(uint64(1)<<previous) != 0 {
			continue
		}
		// Only a successful response fixes the end of an operation. An error
		// response can precede a late publication by the server.
		if operation.Code == "OK" && operation.End < candidate.Begin {
			return false
		}
	}
	return true
}

func validateObservations(observations []Observation) error {
	ids := make(map[uint64]struct{}, len(observations))
	events := make([]uint64, 0, len(observations)*2)
	for _, operation := range observations {
		if operation.ID == 0 || operation.Begin == 0 || operation.End <= operation.Begin || !validCode(operation.Code) || operation.Method == "" {
			return errors.New("invalid history observation")
		}
		if _, duplicate := ids[operation.ID]; duplicate {
			return errors.New("duplicate history operation")
		}
		ids[operation.ID] = struct{}{}
		events = append(events, operation.Begin, operation.End)
	}
	sort.Slice(events, func(left, right int) bool { return events[left] < events[right] })
	for index := 1; index < len(events); index++ {
		if events[index] == events[index-1] {
			return fmt.Errorf("duplicate history event %d", events[index])
		}
	}
	return nil
}

func validCode(value string) bool {
	for code := codes.OK; code <= codes.Unauthenticated; code++ {
		if code.String() == value {
			return true
		}
	}
	return false
}
