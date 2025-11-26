package godb

import "fmt"

type InsertOp struct {
	f DBFile
	c Operator
}

// Construct an insert operator that inserts the records in the child Operator
// into the specified DBFile.
func NewInsertOp(insertFile DBFile, child Operator) *InsertOp {
	return &InsertOp{insertFile, child}
}

// The insert TupleDesc is a one column descriptor with an integer field named "count"
func (i *InsertOp) Descriptor() *TupleDesc {
	return &TupleDesc{
		Fields: []FieldType{
			{"count", "", IntType},
		},
	}
}

// Return an iterator function that inserts all of the tuples from the child
// iterator into the DBFile passed to the constuctor and then returns a
// one-field tuple with a "count" field indicating the number of tuples that
// were inserted.  Tuples should be inserted using the [DBFile.insertTuple]
// method.
func (iop *InsertOp) Iterator(tid TransactionID) (func() (*Tuple, error), error) {
	return func() (*Tuple, error) {
		count := int64(0)
		childIter, err := iop.c.Iterator(tid)
		if err != nil {
			return nil, err
		}
		if childIter == nil {
			return nil, fmt.Errorf("child iterator was nil")
		}

		for {
			tup, err := childIter()
			if err != nil || tup == nil {
				break
			}

			err = iop.f.insertTuple(tup, tid)
			if err != nil {
				break
			}
			count++
		}

		return &Tuple{
			Desc:   *iop.Descriptor(),
			Fields: []DBValue{IntField{count}},
		}, err
	}, nil
}
