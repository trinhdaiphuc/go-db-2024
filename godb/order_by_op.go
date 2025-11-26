package godb

import (
	"fmt"
	"sort"
)

type OrderBy struct {
	orderBy   []Expr // OrderBy should include these two fields (used by parser)
	child     Operator
	ascending []bool
}

// Construct an order by operator. Saves the list of field, child, and ascending
// values for use in the Iterator() method. Here, orderByFields is a list of
// expressions that can be extracted from the child operator's tuples, and the
// ascending bitmap indicates whether the ith field in the orderByFields list
// should be in ascending (true) or descending (false) order.
func NewOrderBy(orderByFields []Expr, child Operator, ascending []bool) (*OrderBy, error) {
	if len(orderByFields) != len(ascending) {
		return nil, fmt.Errorf("length of orderByFields and ascending must be the same")
	}
	return &OrderBy{
		orderBy:   orderByFields,
		child:     child,
		ascending: ascending,
	}, nil // replace me

}

// Return the tuple descriptor.
//
// Note that the order by just changes the order of the child tuples, not the
// fields that are emitted.
func (o *OrderBy) Descriptor() *TupleDesc {
	fields := make([]FieldType, len(o.orderBy))
	for i, expr := range o.orderBy {
		fields[i] = FieldType{
			Fname: expr.GetExprType().Fname,
			Ftype: expr.GetExprType().Ftype,
		}
	}
	return &TupleDesc{
		Fields: fields,
	}
}

// Return a function that iterates through the results of the child iterator in
// ascending/descending order, as specified in the constructor.  This sort is
// "blocking" -- it should first construct an in-memory sorted list of results
// to return, and then iterate through them one by one on each subsequent
// invocation of the iterator function.
//
// Although you are free to implement your own sorting logic, you may wish to
// leverage the go sort package and the [sort.Sort] method for this purpose. To
// use this you will need to implement three methods: Len, Swap, and Less that
// the sort algorithm will invoke to produce a sorted list. See the first
// example, example of SortMultiKeys, and documentation at:
// https://pkg.go.dev/sort
func (o *OrderBy) Iterator(tid TransactionID) (func() (*Tuple, error), error) {
	results := []*Tuple{}

	iter, err := o.child.Iterator(tid)
	if err != nil {
		return nil, err
	}
	for {
		tup, err := iter()
		if err != nil {
			return nil, err
		}
		if tup == nil {
			break
		}
		results = append(results, tup)
	}

	sort.Slice(results, func(i, j int) bool {
		for idx, expr := range o.orderBy {
			valI, err := expr.EvalExpr(results[i])
			if err != nil {
				return false
			}
			valJ, err := expr.EvalExpr(results[j])
			if err != nil {
				return false
			}
			comp := valI.EvalPred(valJ, OpLt)
			if comp {
				return o.ascending[idx]
			}
			if valI.EvalPred(valJ, OpGt) {
				return !o.ascending[idx]
			}
		}
		return false
	})

	ids := 0
	return func() (*Tuple, error) {
		if len(results) == 0 {
			return &Tuple{
				Desc: *o.Descriptor(),
				Rid:  tid,
			}, nil
		}

		if ids >= len(results) {
			return nil, nil
		}
		tup := results[ids]
		ids++
		return tup, nil
	}, nil
}
