package godb

import "fmt"

type LimitOp struct {
	// Required fields for parser
	child     Operator
	limitTups Expr
	// Add additional fields here, if needed
}

// Construct a new limit operator. lim is how many tuples to return and child is
// the child operator.
func NewLimitOp(lim Expr, child Operator) *LimitOp {
	return &LimitOp{child, lim}
}

// Return a TupleDescriptor for this limit.
func (l *LimitOp) Descriptor() *TupleDesc {
	return &TupleDesc{
		Fields: l.child.Descriptor().Fields,
	}
}

// Limit operator implementation. This function should iterate over the results
// of the child iterator, and limit the result set to the first [lim] tuples it
// sees (where lim is specified in the constructor).
func (l *LimitOp) Iterator(tid TransactionID) (func() (*Tuple, error), error) {
	iter, err := l.child.Iterator(tid)
	if err != nil {
		return nil, err
	}
	limitVal, err := l.limitTups.EvalExpr(nil)
	if err != nil {
		return nil, err
	}
	limitInt, ok := limitVal.(IntField)
	if !ok {
		return nil, fmt.Errorf("limit expression did not evaluate to an integer")
	}
	idx := int64(0)
	return func() (*Tuple, error) {
		if idx >= limitInt.Value {
			return nil, nil
		}
		tuple, err := iter()
		if err != nil || tuple == nil {
			return nil, err
		}
		idx++
		return tuple, nil
	}, nil
}
