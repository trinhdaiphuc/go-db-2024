package godb

import (
	"fmt"
)

type DeleteOp struct {
	f DBFile
	o Operator
}

// Construct a delete operator. The delete operator deletes the records in the
// child Operator from the specified DBFile.
func NewDeleteOp(deleteFile DBFile, child Operator) *DeleteOp {
	return &DeleteOp{
		f: deleteFile,
		o: child,
	}
}

// The delete TupleDesc is a one column descriptor with an integer field named
// "count".
func (i *DeleteOp) Descriptor() *TupleDesc {
	return &TupleDesc{
		Fields: []FieldType{
			{
				Fname: "count",
				Ftype: IntType,
			},
		},
	}
}

// Return an iterator that deletes all of the tuples from the child iterator
// from the DBFile passed to the constructor and then returns a one-field tuple
// with a "count" field indicating the number of tuples that were deleted.
// Tuples should be deleted using the [DBFile.deleteTuple] method.
func (dop *DeleteOp) Iterator(tid TransactionID) (func() (*Tuple, error), error) {
	return func() (*Tuple, error) {
		count := int64(0)
		it, err := dop.o.Iterator(tid)
		if err != nil {
			return nil, err
		}
		for {
			tup, err := it()
			if err != nil {
				return nil, err
			}
			if tup == nil {
				break
			}
			err = dop.f.deleteTuple(tup, tid)
			if err != nil {
				return nil, fmt.Errorf("failed to delete tuple: %w", err)
			}
			count++
		}

		return &Tuple{
			Desc:   *dop.Descriptor(),
			Fields: []DBValue{IntField{Value: count}},
		}, nil
	}, nil
}
