package godb

import "fmt"

type Project struct {
	selectFields []Expr // required fields for parser
	outputNames  []string
	child        Operator
	distinct     bool
}

// Construct a projection operator. It saves the list of selected field, child,
// and the child op. Here, selectFields is a list of expressions that represents
// the fields to be selected, outputNames are names by which the selected fields
// are named (should be same length as selectFields; throws error if not),
// distinct is for noting whether the projection reports only distinct results,
// and child is the child operator.
func NewProjectOp(selectFields []Expr, outputNames []string, distinct bool, child Operator) (Operator, error) {
	if len(selectFields) != len(outputNames) {
		return nil, fmt.Errorf("number of select fields and output names must be the same")
	}
	return &Project{
		selectFields: selectFields,
		outputNames:  outputNames,
		child:        child,
		distinct:     distinct,
	}, nil
}

// Return a TupleDescriptor for this projection. The returned descriptor should
// contain fields for each field in the constructor selectFields list with
// outputNames as specified in the constructor.
//
// HINT: you can use expr.GetExprType() to get the field type
func (p *Project) Descriptor() *TupleDesc {
	fields := make([]FieldType, len(p.selectFields))
	for i, expr := range p.selectFields {
		fields[i] = FieldType{
			Fname: p.outputNames[i],
			Ftype: expr.GetExprType().Ftype,
		}
	}
	return &TupleDesc{
		Fields: fields,
	}

}

// Project operator implementation. This function should iterate over the
// results of the child iterator, projecting out the fields from each tuple. In
// the case of distinct projection, duplicate tuples should be removed. To
// implement this you will need to record in some data structure with the
// distinct tuples seen so far. Note that support for the distinct keyword is
// optional as specified in the lab 2 assignment.
func (p *Project) Iterator(tid TransactionID) (func() (*Tuple, error), error) {
	distinctSet := make(map[any]struct{})
	childIter, err := p.child.Iterator(tid)
	if err != nil {
		return nil, err
	}
	return func() (*Tuple, error) {
		for {
			tuple, err := childIter()
			if err != nil || tuple == nil {
				return nil, err
			}

			fields := make([]DBValue, len(p.selectFields))
			for i, expr := range p.selectFields {
				val, err := expr.EvalExpr(tuple)
				if err != nil {
					return nil, err
				}
				fields[i] = val
			}
			if p.distinct {
				tupleKey := tuple.tupleKey()
				if _, exists := distinctSet[tupleKey]; exists {
					continue // skip duplicate
				}
				distinctSet[tupleKey] = struct{}{}
				return &Tuple{
					Desc:   *p.Descriptor(),
					Fields: fields,
				}, nil
			} else {
				return &Tuple{
					Desc:   *p.Descriptor(),
					Fields: fields,
				}, nil
			}
		}
	}, nil
}
