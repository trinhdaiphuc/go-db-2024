package godb

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"unsafe"
)

/* HeapPage implements the Page interface for pages of HeapFiles. We have
provided our interface to HeapPage below for you to fill in, but you are not
required to implement these methods except for the three methods that the Page
interface requires.  You will want to use an interface like what we provide to
implement the methods of [HeapFile] that insert, delete, and iterate through
tuples.

In GoDB all tuples are fixed length, which means that given a TupleDesc it is
possible to figure out how many tuple "slots" fit on a given page.

In addition, all pages are PageSize bytes.  They begin with a header with a 32
bit integer with the number of slots (tuples), and a second 32 bit integer with
the number of used slots.

Each tuple occupies the same number of bytes.  You can use the go function
unsafe.Sizeof() to determine the size in bytes of an object.  So, a GoDB integer
(represented as an int64) requires unsafe.Sizeof(int64(0)) bytes.  For strings,
we encode them as byte arrays of StringLength, so they are size
((int)(unsafe.Sizeof(byte('a')))) * StringLength bytes.  The size in bytes  of a
tuple is just the sum of the size in bytes of its fields.

Once you have figured out how big a record is, you can determine the number of
slots on on the page as:

remPageSize = PageSize - 8 // bytes after header
numSlots = remPageSize / bytesPerTuple //integer division will round down

To serialize a page to a buffer, you can then:

write the number of slots as an int32
write the number of used slots as an int32
write the tuples themselves to the buffer

You will follow the inverse process to read pages from a buffer.

Note that to process deletions you will likely delete tuples at a specific
position (slot) in the heap page.  This means that after a page is read from
disk, tuples should retain the same slot number. Because GoDB will never evict a
dirty page, it's OK if tuples are renumbered when they are written back to disk.

*/

type heapPage struct {
	pageNo  int
	numSlot int
	desc    *TupleDesc
	f       *HeapFile
	tuples  []Tuple
	dirty   bool
}

type ridHeapPageSlot struct {
	pageNo int
	slot   int
}

// Construct a new heap page
func newHeapPage(desc *TupleDesc, pageNo int, f *HeapFile) (*heapPage, error) {
	bytesPerTuple := 0
	for _, field := range desc.Fields {
		switch field.Ftype {
		case IntType:
			bytesPerTuple += int(unsafe.Sizeof(int64(0)))
		case StringType:
			bytesPerTuple += StringLength * int(unsafe.Sizeof(byte('a')))
		default:
			return nil, fmt.Errorf("unknown field type in tuple descriptor")
		}
	}
	return &heapPage{
		pageNo:  pageNo,
		numSlot: (PageSize - 8) / bytesPerTuple,
		desc:    desc,
		f:       f,
		tuples:  make([]Tuple, 0),
		dirty:   false,
	}, nil
}

func (h *heapPage) getNumSlots() int {
	return h.numSlot
}

// Insert the tuple into a free slot on the page, or return an error if there are
// no free slots.  Set the tuples rid and return it.
func (h *heapPage) insertTuple(t *Tuple) (recordID, error) {
	if len(h.tuples) >= h.getNumSlots() {
		return nil, GoDBError{
			code:      PageFullError,
			errString: "No free slots on page",
		}
	}
	t.Rid = ridHeapPageSlot{
		pageNo: h.pageNo,
		slot:   len(h.tuples),
	}
	h.tuples = append(h.tuples, *t)
	return ridHeapPageSlot{
		pageNo: h.pageNo,
		slot:   len(h.tuples) - 1,
	}, nil
}

// Delete the tuple at the specified record ID, or return an error if the ID is
// invalid.
func (h *heapPage) deleteTuple(rid recordID) error {
	if len(h.tuples) == 0 {
		return GoDBError{
			code:      TupleNotFoundError,
			errString: "No tuples on page",
		}
	}
	ridHeap, ok := rid.(ridHeapPageSlot)
	if !ok {
		return GoDBError{
			code:      TupleNotFoundError,
			errString: "Invalid recordID type for heapPage",
		}
	}
	if ridHeap.pageNo != h.pageNo || ridHeap.slot < 0 {
		return GoDBError{
			code:      TupleNotFoundError,
			errString: fmt.Sprintf("Invalid recordID for this page. rid: %v, tuples: %d", ridHeap, len(h.tuples)),
		}
	}

	for i := 0; i < len(h.tuples); i++ {
		ridCheck := h.tuples[i].Rid.(ridHeapPageSlot)
		if ridCheck.pageNo == ridHeap.pageNo && ridCheck.slot == ridHeap.slot {
			h.tuples = append(h.tuples[:i], h.tuples[i+1:]...)
			return nil
		}
	}
	return GoDBError{
		code:      TupleNotFoundError,
		errString: fmt.Sprintf("Tuple with rid %v not found on page", ridHeap),
	}
}

// Page method - return whether or not the page is dirty
func (h *heapPage) isDirty() bool {
	return h.dirty
}

// Page method - mark the page as dirty
func (h *heapPage) setDirty(tid TransactionID, dirty bool) {
	h.dirty = dirty
}

// Page method - return the corresponding HeapFile
// for this page.
func (p *heapPage) getFile() DBFile {
	return p.f
}

// Allocate a new bytes.Buffer and write the heap page to it. Returns an error
// if the write to the the buffer fails. You will likely want to call this from
// your [HeapFile.flushPage] method.  You should write the page header, using
// the binary.Write method in LittleEndian order, followed by the tuples of the
// page, written using the Tuple.writeTo method.
func (h *heapPage) toBuffer() (*bytes.Buffer, error) {
	buffer := bytes.NewBuffer(make([]byte, 0, PageSize)) // Preallocate PageSize bytes

	// Write header
	err := binary.Write(buffer, binary.LittleEndian, int32(h.getNumSlots()))
	if err != nil {
		return nil, err
	}

	err = binary.Write(buffer, binary.LittleEndian, int32(len(h.tuples)))
	if err != nil {
		return nil, err
	}

	// Write tuples
	for _, tuple := range h.tuples {
		err := tuple.writeTo(buffer)
		if err != nil {
			return nil, err
		}
	}

	if buffer.Len() > PageSize {
		return nil, fmt.Errorf("heapPage.toBuffer: buffer size %d exceeds PageSize %d", buffer.Len(), PageSize)
	}

	// Pad the buffer to PageSize if necessary
	for buffer.Len() < PageSize {
		err := binary.Write(buffer, binary.LittleEndian, byte(0))
		if err != nil {
			return nil, err
		}
	}

	return buffer, nil
}

// Read the contents of the HeapPage from the supplied buffer.
func (h *heapPage) initFromBuffer(buf *bytes.Buffer) error {
	var numPage, usedSlots int32
	err := binary.Read(buf, binary.LittleEndian, &numPage)
	if err != nil {
		return err
	}

	err = binary.Read(buf, binary.LittleEndian, &usedSlots)
	if err != nil {
		return err
	}

	h.tuples = make([]Tuple, 0, usedSlots)
	for i := 0; i < int(usedSlots); i++ {
		tuple, err := readTupleFrom(buf, h.desc)
		if err != nil {
			return err
		}
		tuple.Rid = ridHeapPageSlot{
			pageNo: h.pageNo,
			slot:   i,
		}
		h.tuples = append(h.tuples, *tuple)
	}

	return nil
}

// Return a function that iterates through the tuples of the heap page.  Be sure
// to set the rid of the tuple to the rid struct of your choosing beforing
// return it. Return nil, nil when the last tuple is reached.
func (p *heapPage) tupleIter() func() (*Tuple, error) {
	i := 0
	return func() (*Tuple, error) {
		if i >= len(p.tuples) {
			return nil, nil
		}

		defer func() { i++ }()

		return &p.tuples[i], nil
	}
}
