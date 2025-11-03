package godb

import (
	"bufio"
	"bytes"
	"fmt"
	"os"
	"strconv"
	"strings"
)

// A HeapFile is an unordered collection of tuples.
//
// HeapFile is a public class because external callers may wish to instantiate
// database tables using the method [LoadFromCSV]
type HeapFile struct {
	fromFile string
	file     *os.File
	td       *TupleDesc
	bufPool  *BufferPool
}

// Create a HeapFile.
// Parameters
// - fromFile: backing file for the HeapFile.  May be empty or a previously created heap file.
// - td: the TupleDesc for the HeapFile.
// - bp: the BufferPool that is used to store pages read from the HeapFile
// May return an error if the file cannot be opened or created.
func NewHeapFile(fromFile string, td *TupleDesc, bp *BufferPool) (*HeapFile, error) {
	file, err := os.OpenFile(fromFile, os.O_RDWR|os.O_CREATE, 0666)
	if err != nil {
		return nil, err
	}

	return &HeapFile{
		fromFile: fromFile,
		file:     file,
		td:       td,
		bufPool:  bp,
	}, nil
}

// Return the name of the backing file
func (f *HeapFile) BackingFile() string {
	return f.fromFile
}

// Return the number of pages in the heap file
func (f *HeapFile) NumPages() int {
	stat, err := f.file.Stat()
	if err != nil {
		return 0
	}
	return int(stat.Size() / int64(PageSize))
}

// Load the contents of a heap file from a specified CSV file.  Parameters are as follows:
// - hasHeader:  whether or not the CSV file has a header
// - sep: the character to use to separate fields
// - skipLastField: if true, the final field is skipped (some TPC datasets include a trailing separator on each line)
// Returns an error if the field cannot be opened or if a line is malformed
// We provide the implementation of this method, but it won't work until
// [HeapFile.insertTuple] and some other utility functions are implemented
func (f *HeapFile) LoadFromCSV(file *os.File, hasHeader bool, sep string, skipLastField bool) error {
	scanner := bufio.NewScanner(file)
	cnt := 0
	for scanner.Scan() {
		line := scanner.Text()
		fields := strings.Split(line, sep)
		if skipLastField {
			fields = fields[0 : len(fields)-1]
		}
		numFields := len(fields)
		cnt++
		desc := f.Descriptor()
		if desc == nil || desc.Fields == nil {
			return GoDBError{MalformedDataError, "Descriptor was nil"}
		}
		if numFields != len(desc.Fields) {
			return GoDBError{
				MalformedDataError,
				fmt.Sprintf("LoadFromCSV:  line %d (%s) does not have expected number of fields (expected %d, got %d)", cnt, line, len(f.Descriptor().Fields), numFields),
			}
		}
		if cnt == 1 && hasHeader {
			continue
		}
		var newFields []DBValue
		for fno, field := range fields {
			switch f.Descriptor().Fields[fno].Ftype {
			case IntType:
				field = strings.TrimSpace(field)
				floatVal, err := strconv.ParseFloat(field, 64)
				if err != nil {
					return GoDBError{
						TypeMismatchError,
						fmt.Sprintf("LoadFromCSV: couldn't convert value %s to int, tuple %d", field, cnt),
					}
				}
				intValue := int(floatVal)
				newFields = append(newFields, IntField{int64(intValue)})
			case StringType:
				if len(field) > StringLength {
					field = field[0:StringLength]
				}
				newFields = append(newFields, StringField{field})
			}
		}
		newT := Tuple{*f.Descriptor(), newFields, nil}
		tid := NewTID()
		bp := f.bufPool
		f.insertTuple(&newT, tid)

		// Force dirty pages to disk. CommitTransaction may not be implemented
		// yet if this is called in lab 1 or 2.
		bp.FlushAllPages()

	}
	return nil
}

// Read the specified page number from the HeapFile on disk. This method is
// called by the [BufferPool.GetPage] method when it cannot find the page in its
// cache.
//
// This method will need to open the file supplied to the constructor, seek to
// the appropriate offset, read the bytes in, and construct a [heapPage] object,
// using the [heapPage.initFromBuffer] method.
func (f *HeapFile) readPage(pageNo int) (Page, error) {
	if pageNo < 0 || pageNo >= f.NumPages() {
		return nil, GoDBError{
			code:      PageFullError,
			errString: fmt.Sprintf("Requested page number %d out of bounds (num pages %d)", pageNo, f.NumPages()),
		}
	}

	offset := int64(pageNo * PageSize)
	buf := make([]byte, PageSize)
	_, err := f.file.ReadAt(buf, offset)
	if err != nil {
		return nil, err
	}

	hp, err := newHeapPage(f.td, pageNo, f)
	err = hp.initFromBuffer(bytes.NewBuffer(buf))

	return hp, err
}

// Add the tuple to the HeapFile. This method should search through pages in the
// heap file, looking for empty slots and adding the tuple in the first empty
// slot if finds.
//
// If none are found, it should create a new [heapPage] and insert the tuple
// there, and write the heapPage to the end of the HeapFile (e.g., using the
// [flushPage] method.)
//
// To iterate through pages, it should use the [BufferPool.GetPage method]
// rather than directly reading pages itself. For lab 1, you do not need to
// worry about concurrent transactions modifying the Page or HeapFile. We will
// add support for concurrent modifications in lab 3.
//
// The page the tuple is inserted into should be marked as dirty.
func (f *HeapFile) insertTuple(t *Tuple, tid TransactionID) error {
	for pageNo := 0; pageNo < f.NumPages(); pageNo++ {
		page, err := f.bufPool.GetPage(f, pageNo, tid, WritePerm)
		if err != nil {
			return err
		}
		hp := page.(*heapPage)
		_, err = hp.insertTuple(t)
		if err == nil {
			hp.setDirty(tid, true)
			return nil
		}
	}

	// No empty slots found; create a new page
	newPageNo := f.NumPages()
	hp, err := newHeapPage(f.td, newPageNo, f)
	if err != nil {
		return err
	}
	_, err = hp.insertTuple(t)
	if err != nil {
		return err
	}
	hp.setDirty(tid, true)

	// Write the new page to the end of the file
	err = f.flushPage(hp)
	return err
}

// Remove the provided tuple from the HeapFile.
//
// This method should use the [Tuple.Rid] field of t to determine which tuple to
// remove. The Rid field should be set when the tuple is read using the
// [Iterator] method, or is otherwise created (as in tests). Note that Rid is an
// empty interface, so you can supply any object you wish. You will likely want
// to identify the heap page and slot within the page that the tuple came from.
//
// The page the tuple is deleted from should be marked as dirty.
func (f *HeapFile) deleteTuple(t *Tuple, tid TransactionID) error {
	for pageNo := 0; pageNo < f.NumPages(); pageNo++ {
		page, err := f.bufPool.GetPage(f, pageNo, tid, WritePerm)
		if err != nil {
			return err
		}
		hp := page.(*heapPage)
		err = hp.deleteTuple(t.Rid)
		if err == nil {
			hp.setDirty(tid, true)
			return nil
		}
	}
	return nil
}

// Method to force the specified page back to the backing file at the
// appropriate location. This will be called by BufferPool when it wants to
// evict a page. The Page object should store information about its offset on
// disk (e.g., that it is the ith page in the heap file), so you can determine
// where to write it back.
func (f *HeapFile) flushPage(p Page) error {
	hp := p.(*heapPage)
	buf, err := hp.toBuffer()
	if err != nil {
		return err
	}

	offset := int64(hp.pageNo * PageSize)
	_, err = f.file.WriteAt(buf.Bytes(), offset)
	if err != nil {
		return err
	}

	return nil
}

// [Operator] descriptor method -- return the TupleDesc for this HeapFile
// Supplied as argument to NewHeapFile.
func (f *HeapFile) Descriptor() *TupleDesc {
	return f.td

}

// [Operator] iterator method
// Return a function that iterates through the records in the heap file
// Note that this method should read pages from the HeapFile using the
// BufferPool method GetPage, rather than reading pages directly,
// since the BufferPool caches pages and manages page-level locking state for
// transactions
// You should esnure that Tuples returned by this method have their Rid object
// set appropriate so that [deleteTuple] will work (see additional comments there).
// Make sure to set the returned tuple's TupleDescriptor to the TupleDescriptor of
// the HeapFile. This allows it to correctly capture the table qualifier.
func (f *HeapFile) Iterator(tid TransactionID) (func() (*Tuple, error), error) {
	pageNo := 0
	var iterTuple func() (*Tuple, error)
	return func() (*Tuple, error) {
		if iterTuple != nil {
			tuple, err := iterTuple()
			if err != nil {
				return nil, err
			}
			if tuple != nil {
				return tuple, nil
			}
			// else, move to next page
			iterTuple = nil
			pageNo++
		}

		if pageNo >= f.NumPages() {
			return nil, nil // end of file
		}

		page, err := f.bufPool.GetPage(f, pageNo, tid, ReadPerm)
		if err != nil {
			return nil, err
		}
		hp := page.(*heapPage)

		iterTuple = hp.tupleIter()
		return iterTuple()
	}, nil
}

// internal strucuture to use as key for a heap page
type heapHash struct {
	FileName string
	PageNo   int
}

// This method returns a key for a page to use in a map object, used by
// BufferPool to determine if a page is cached or not.  We recommend using a
// heapHash struct as the key for a page, although you can use any struct that
// does not contain a slice or a map that uniquely identifies the page.
func (f *HeapFile) pageKey(pgNo int) any {
	return heapHash{
		FileName: f.fromFile,
		PageNo:   pgNo,
	}
}
