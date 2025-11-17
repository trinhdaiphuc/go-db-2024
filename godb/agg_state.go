package godb

// interface for an aggregation state
type AggState interface {
	// Initializes an aggregation state. Is supplied with an alias, an expr to
	// evaluate an input tuple into a DBValue, and a getter to extract from the
	// DBValue its int or string field's value.
	Init(alias string, expr Expr) error

	// Makes an copy of the aggregation state.
	Copy() AggState

	// Adds an tuple to the aggregation state.
	AddTuple(*Tuple)

	// Returns the final result of the aggregation as a tuple.
	Finalize() *Tuple

	// Gets the tuple description of the tuple that Finalize() returns.
	GetTupleDesc() *TupleDesc
}

// Implements the aggregation state for COUNT
// We are supplying the implementation of CountAggState as an example. You need to
// implement the rest of the aggregation states.
type CountAggState struct {
	alias string
	expr  Expr
	count int
}

func (a *CountAggState) Copy() AggState {
	return &CountAggState{a.alias, a.expr, a.count}
}

func (a *CountAggState) Init(alias string, expr Expr) error {
	a.count = 0
	a.expr = expr
	a.alias = alias
	return nil
}

func (a *CountAggState) AddTuple(t *Tuple) {
	a.count++
}

func (a *CountAggState) Finalize() *Tuple {
	td := a.GetTupleDesc()
	f := IntField{int64(a.count)}
	fs := []DBValue{f}
	t := Tuple{*td, fs, nil}
	return &t
}

func (a *CountAggState) GetTupleDesc() *TupleDesc {
	ft := FieldType{a.alias, "", IntType}
	fts := []FieldType{ft}
	td := TupleDesc{}
	td.Fields = fts
	return &td
}

// Implements the aggregation state for SUM
type SumAggState struct {
	alias string
	expr  Expr
	sum   int64
}

func (a *SumAggState) Copy() AggState {
	return &SumAggState{
		alias: a.alias,
		expr:  a.expr,
		sum:   a.sum,
	}
}

func intAggGetter(v DBValue) any {
	// TODO: some code goes here
	return nil // replace me
}

func stringAggGetter(v DBValue) any {
	// TODO: some code goes here
	return nil // replace me
}

func (a *SumAggState) Init(alias string, expr Expr) error {
	a.alias = alias
	a.expr = expr
	a.sum = 0
	return nil
}

func (a *SumAggState) AddTuple(t *Tuple) {
	val, err := a.expr.EvalExpr(t)
	if err != nil {
		return
	}

	intVal, ok := val.(IntField)
	if !ok {
		return
	}
	a.sum += intVal.Value
}

func (a *SumAggState) GetTupleDesc() *TupleDesc {
	ft := FieldType{a.alias, "", IntType}
	fts := []FieldType{ft}
	td := TupleDesc{}
	td.Fields = fts
	return &td
}

func (a *SumAggState) Finalize() *Tuple {
	td := a.GetTupleDesc()
	f := IntField{a.sum}
	fs := []DBValue{f}
	t := Tuple{*td, fs, nil}
	return &t
}

// Implements the aggregation state for AVG
// Note that we always AddTuple() at least once before Finalize()
// so no worries for divide-by-zero
type AvgAggState struct {
	alias string
	expr  Expr
	sum   int64
	count int64
}

func (a *AvgAggState) Copy() AggState {
	return &AvgAggState{
		alias: a.alias,
		expr:  a.expr,
		sum:   a.sum,
		count: a.count,
	}
}

func (a *AvgAggState) Init(alias string, expr Expr) error {
	a.alias = alias
	a.expr = expr
	a.sum = 0
	a.count = 0
	return nil
}

func (a *AvgAggState) AddTuple(t *Tuple) {
	val, err := a.expr.EvalExpr(t)
	if err != nil {
		return
	}
	intVal, ok := val.(IntField)
	if !ok {
		return
	}
	a.sum += intVal.Value
	a.count++
}

func (a *AvgAggState) GetTupleDesc() *TupleDesc {
	ft := FieldType{a.alias, "", IntType}
	fts := []FieldType{ft}
	td := TupleDesc{}
	td.Fields = fts
	return &td
}

func (a *AvgAggState) Finalize() *Tuple {
	td := a.GetTupleDesc()
	f := IntField{a.sum / a.count}
	fs := []DBValue{f}
	t := Tuple{*td, fs, nil}
	return &t
}

// Implements the aggregation state for MAX
// Note that we always AddTuple() at least once before Finalize()
// so no worries for NaN max
type MaxAggState struct {
	alias  string
	expr   Expr
	maxVal DBValue
}

func (a *MaxAggState) Copy() AggState {
	return &MaxAggState{
		alias:  a.alias,
		expr:   a.expr,
		maxVal: a.maxVal,
	}
}

func (a *MaxAggState) Init(alias string, expr Expr) error {
	a.alias = alias
	a.expr = expr
	a.maxVal = nil
	return nil
}

func (a *MaxAggState) AddTuple(t *Tuple) {
	val, err := a.expr.EvalExpr(t)
	if err != nil {
		return
	}

	if a.maxVal == nil {
		a.maxVal = val
		return
	}

	if val.EvalPred(a.maxVal, OpGt) {
		a.maxVal = val
	}
}

func (a *MaxAggState) GetTupleDesc() *TupleDesc {
	var fType DBType
	switch a.maxVal.(type) {
	case IntField:
		fType = IntType
	case StringField:
		fType = StringType
	}
	ft := FieldType{a.alias, "", fType}
	fts := []FieldType{ft}
	td := TupleDesc{}
	td.Fields = fts
	return &td
}

func (a *MaxAggState) Finalize() *Tuple {
	td := a.GetTupleDesc()
	fs := []DBValue{a.maxVal}
	t := Tuple{*td, fs, nil}
	return &t
}

// Implements the aggregation state for MIN
// Note that we always AddTuple() at least once before Finalize()
// so no worries for NaN min
type MinAggState struct {
	alias  string
	expr   Expr
	minVal DBValue
}

func (a *MinAggState) Copy() AggState {
	return &MinAggState{
		alias:  a.alias,
		expr:   a.expr,
		minVal: a.minVal,
	}
}

func (a *MinAggState) Init(alias string, expr Expr) error {
	a.alias = alias
	a.expr = expr
	a.minVal = nil
	return nil
}

func (a *MinAggState) AddTuple(t *Tuple) {
	val, err := a.expr.EvalExpr(t)
	if err != nil {
		return
	}

	if a.minVal == nil {
		a.minVal = val
		return
	}

	if val.EvalPred(a.minVal, OpLt) {
		a.minVal = val
	}
}

func (a *MinAggState) GetTupleDesc() *TupleDesc {
	var fType DBType
	switch a.minVal.(type) {
	case IntField:
		fType = IntType
	case StringField:
		fType = StringType
	}
	ft := FieldType{a.alias, "", fType}
	fts := []FieldType{ft}
	td := TupleDesc{}
	td.Fields = fts
	return &td
}

func (a *MinAggState) Finalize() *Tuple {
	td := a.GetTupleDesc()
	fs := []DBValue{a.minVal}
	t := Tuple{*td, fs, nil}
	return &t
}
