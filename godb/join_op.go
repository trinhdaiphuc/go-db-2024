package godb

import (
	"fmt"
	"time"
)

type EqualityJoin struct {
	// Expressions that when applied to tuples from the left or right operators,
	// respectively, return the value of the left or right side of the join
	leftField, rightField Expr

	left, right *Operator // Operators for the two inputs of the join

	// The maximum number of records of intermediate state that the join should
	// use (only required for optional exercise).
	maxBufferSize int
}

// Constructor for a join of integer expressions.
//
// Returns an error if either the left or right expression is not an integer.
func NewJoin(left Operator, leftField Expr, right Operator, rightField Expr, maxBufferSize int) (*EqualityJoin, error) {
	return &EqualityJoin{leftField, rightField, &left, &right, maxBufferSize}, nil
}

// Return a TupleDesc for this join. The returned descriptor should contain the
// union of the fields in the descriptors of the left and right operators.
//
// HINT: use [TupleDesc.merge].
func (hj *EqualityJoin) Descriptor() *TupleDesc {
	tdRight := (*hj.right).Descriptor()
	tdLeft := (*hj.left).Descriptor()
	return tdRight.merge(tdLeft)
}

// Join operator implementation. This function should iterate over the results
// of the join. The join should be the result of joining joinOp.left and
// joinOp.right, applying the joinOp.leftField and joinOp.rightField expressions
// to the tuples of the left and right iterators respectively, and joining them
// using an equality predicate.
//
// HINT: When implementing the simple nested loop join, you should keep in mind
// that you only iterate through the left iterator once (outer loop) but iterate
// through the right iterator once for every tuple in the left iterator (inner
// loop).
//
// HINT: You can use [Tuple.joinTuples] to join two tuples.
//
// OPTIONAL EXERCISE: the operator implementation should not use more than
// maxBufferSize records, and should pass the testBigJoin test without timing
// out. To pass this test, you will need to use something other than a nested
// loops join.
func (joinOp *EqualityJoin) Iterator(tid TransactionID) (func() (*Tuple, error), error) {
	return joinOp.hashJoin(tid)
	// return joinOp.nestedLoopJoin(tid)
	// return joinOp.sortMergeJoin(tid)
}

func (joinOp *EqualityJoin) nestedLoopJoin(tid TransactionID) (func() (*Tuple, error), error) {
	leftIter, err := (*joinOp.left).Iterator(tid)
	if err != nil {
		return nil, err
	}
	rightIter, err := (*joinOp.right).Iterator(tid)
	if err != nil || rightIter == nil {
		return nil, err
	}
	var (
		rightTuple *Tuple
		rightValue DBValue
	)
	leftTuple, err := leftIter()
	if err != nil || leftTuple == nil {
		return nil, err
	}
	leftValue, err := joinOp.leftField.EvalExpr(leftTuple)
	if err != nil {
		return nil, err
	}
	return func() (*Tuple, error) {
		for {
			rightTuple, err = rightIter()
			if err != nil {
				return nil, err
			}

			if rightTuple == nil {
				leftTuple, err = leftIter()
				if err != nil || leftTuple == nil {
					return nil, err
				}
				leftValue, err = joinOp.leftField.EvalExpr(leftTuple)
				if err != nil {
					return nil, err
				}
				rightIter, err = (*joinOp.right).Iterator(tid)
				if err != nil {
					return nil, err
				}
				continue
			}

			rightValue, err = joinOp.rightField.EvalExpr(rightTuple)
			if err != nil {
				return nil, err
			}

			if rightValue.EvalPred(leftValue, OpEq) {
				return joinTuples(leftTuple, rightTuple), nil
			}
		}
	}, nil
}

func (joinOp *EqualityJoin) hashJoin(tid TransactionID) (func() (*Tuple, error), error) {
	leftIter, err := (*joinOp.left).Iterator(tid)
	if err != nil {
		return nil, err
	}

	// State for the iterator closure
	var (
		leftDone bool
		results  []*Tuple
		resIndex int
		// We keep leftIter to continue reading chunks
	)

	// helper to build a chunk hash-table from left input and probe right
	fillChunkAndProbe := func() error {
		// Build hash table from up to maxBufferSize left tuples.
		ht := make(map[string][]*Tuple)
		cnt := 0
		for {
			if joinOp.maxBufferSize > 0 && cnt >= joinOp.maxBufferSize {
				break
			}
			lt, err := leftIter()
			if err != nil {
				return err
			}
			if lt == nil {
				// no more left tuples
				leftDone = true
				break
			}
			keyVal, err := joinOp.leftField.EvalExpr(lt)
			if err != nil {
				return err
			}
			keyStr := fmt.Sprintf("%#v", keyVal)
			ht[keyStr] = append(ht[keyStr], lt)
			cnt++
		}

		// If hash table empty and left is exhausted, nothing more to do.
		if len(ht) == 0 {
			return nil
		}

		// Probe right side: scan full right iterator and produce joined tuples.
		rightIter, err := (*joinOp.right).Iterator(tid)
		if err != nil {
			return err
		}

		// Clear previous results slice
		results = results[:0]
		resIndex = 0

		for {
			rt, err := rightIter()
			if err != nil {
				return err
			}
			if rt == nil {
				break
			}
			rk, err := joinOp.rightField.EvalExpr(rt)
			if err != nil {
				return err
			}
			keyStr := fmt.Sprintf("%#v", rk)
			if lhs, ok := ht[keyStr]; ok {
				for _, l := range lhs {
					results = append(results, joinTuples(l, rt))
				}
			}
		}
		return nil
	}

	// Initialize first chunk
	if err := fillChunkAndProbe(); err != nil {
		return nil, err
	}

	return func() (*Tuple, error) {
		// If we have buffered results, return them one by one.
		if resIndex < len(results) {
			t := results[resIndex]
			resIndex++
			return t, nil
		}

		// If no results and left is exhausted, we're done.
		if leftDone {
			return nil, nil
		}

		// Otherwise, fill next chunk and probe again.
		if err := fillChunkAndProbe(); err != nil {
			return nil, err
		}

		// After filling, if still no results and left exhausted, end.
		if resIndex >= len(results) {
			if leftDone {
				return nil, nil
			}
			// It is possible to have an empty result for this chunk (no matching right tuples).
			// Loop to get the next chunk until we either produce a result or leftDone.
			for {
				if leftDone {
					return nil, nil
				}
				if err := fillChunkAndProbe(); err != nil {
					return nil, err
				}
				if resIndex < len(results) {
					break
				}
			}
		}

		// Now return next result
		if resIndex < len(results) {
			t := results[resIndex]
			resIndex++
			return t, nil
		}
		return nil, nil
	}, nil
}

func (joinOp *EqualityJoin) sortMergeJoin(tid TransactionID) (func() (*Tuple, error), error) {
	leftSorted, err := sortHeapFile(tid, *joinOp.left, joinOp.leftField, joinOp.maxBufferSize)
	if err != nil {
		return nil, err
	}

	rightSorted, err := sortHeapFile(tid, *joinOp.right, joinOp.rightField, joinOp.maxBufferSize)
	if err != nil {
		return nil, err
	}

	var (
		leftIndex  int
		rightIndex int
	)

	return func() (*Tuple, error) {
		for leftIndex < len(leftSorted) {
			if rightIndex >= len(rightSorted) {
				// Reset right index and move left forward
				rightIndex = 0
				leftIndex++
				continue
			}

			leftTuple := leftSorted[leftIndex]
			rightTuple := rightSorted[rightIndex]

			leftValue, err := joinOp.leftField.EvalExpr(leftTuple)
			if err != nil {
				return nil, err
			}
			rightValue, err := joinOp.rightField.EvalExpr(rightTuple)
			if err != nil {
				return nil, err
			}

			if leftValue.EvalPred(rightValue, OpEq) {
				// Match found
				result := joinTuples(leftTuple, rightTuple)
				rightIndex++
				return result, nil
			} else if leftValue.EvalPred(rightValue, OpLt) {
				leftIndex++
			} else {
				rightIndex++
			}
		}
		return nil, nil
	}, nil

}

func sortHeapFile(tid TransactionID, hf Operator, sortField Expr, maxBufferSize int) ([]*Tuple, error) {
	iter, err := hf.Iterator(tid)
	if err != nil {
		return nil, err
	}
	tup, err := iter()
	if err != nil {
		return nil, err
	}
	tuples := []*Tuple{tup}

	start := time.Now()
	defer func() {
		elapsed := time.Since(start)
		fmt.Printf("Sorting heap file took %s\n", elapsed)
	}()
	for {
		tup, err := iter()
		if err != nil {
			return nil, err
		}
		if tup == nil {
			break
		}
		i := 0
		for ; i < len(tuples); i++ {
			tupVal, err := sortField.EvalExpr(tup)
			if err != nil {
				return nil, err
			}

			tupValI, err := sortField.EvalExpr(tuples[i])
			if err != nil {
				return nil, err
			}

			if tupVal.EvalPred(tupValI, OpLt) {
				tuples = append(tuples[:i], append([]*Tuple{tup}, tuples[i:]...)...)
				break
			}
		}
		if i == len(tuples) {
			tuples = append(tuples, tup)
		}
	}
	return tuples, nil
}
