package colgatedb.dbfile;

import colgatedb.BufferManager;
import colgatedb.Database;
import colgatedb.DbException;
import colgatedb.page.*;
import colgatedb.transactions.TransactionAbortedException;
import colgatedb.transactions.TransactionId;
import colgatedb.tuple.RecordId;
import colgatedb.tuple.Tuple;
import colgatedb.tuple.TupleDesc;

import java.util.Iterator;
import java.util.NoSuchElementException;

/**
 * ColgateDB
 * @author Michael Hay mhay@colgate.edu
 * <p>
 * ColgateDB was developed by Michael Hay but borrows considerably from past
 * efforts including SimpleDB (developed by Sam Madden at MIT) and its predecessor
 * Minibase (developed at U. of Wisconsin by Raghu Ramakrishnan).
 * <p>
 * The contents of this file are either wholly the creation of Michael Hay or are
 * a significant adaptation of code from the SimpleDB project.  A number of
 * substantive changes have been made to meet the pedagogical goals of the cosc460
 * course at Colgate.  If this file contains remnants from SimpleDB, we are
 * grateful for Sam's permission to use and adapt his materials.
 */

/**
 * HeapFile is an implementation of a DbFile that stores a collection of tuples
 * in no particular order. Tuples are stored on pages, each of which is a fixed
 * size, and the file is simply a collection of those pages. HeapFile works
 * closely with SlottedPage. The format of SlottedPages is described in the javadocs
 * for SlottedPage.
 *
 * @see SlottedPage
 */
public class HeapFile implements DbFile {

    private final SlottedPageMaker pageMaker;   // this should be initialized in constructor
    private TupleDesc td;
    private int pageSize;
    private int tableId;
    private int numPages;
    private BufferManager bm;

    /**
     * Creates a heap file.
     * @param td the schema for records stored in this heapfile
     * @param pageSize the size in bytes of pages stored on disk (needed for PageMaker)
     * @param tableid the unique id for this table (needed to create appropriate page ids)
     * @param numPages size of this heapfile (i.e., number of pages already stored on disk)
     */
    public HeapFile(TupleDesc td, int pageSize, int tableid, int numPages) {
        this.tableId = tableid;
        this.td = td;
        this.pageSize = pageSize;
        this.numPages = numPages;
        this.pageMaker = new SlottedPageMaker(td, pageSize);
        this.bm = Database.getBufferManager();
    }

    /**
     * Returns the number of pages in this HeapFile.
     */
    public int numPages() {
        return this.numPages;
    }

    @Override
    public int getId() {
        return this.tableId;
    }

    @Override
    public TupleDesc getTupleDesc() {
        return this.td;
    }

    @Override
    public void insertTuple(TransactionId tid, Tuple t) throws TransactionAbortedException {
        PageId pid = null;
        SlottedPage page = null;
        boolean foundPage = false;
        if(numPages > 0) {
            for(int i = 0; i < numPages; i++) { //find a page that has empty slots
                pid = new SimplePageId(tableId, i);
                page = (SlottedPage) bm.pinPage(pid, pageMaker);
                if (page.getNumEmptySlots() != 0) {
                    foundPage = true;
                }
                bm.unpinPage(pid, false);
                if (foundPage){
                    break;
                }
            }
        }
        if (!foundPage) { //if there's no page or all pages are full, allocate new page
            pid = new SimplePageId(tableId, numPages++);
            bm.allocatePage(pid);
        }
        page = (SlottedPage) bm.pinPage(pid, pageMaker);
        page.insertTuple(t);
        bm.unpinPage(pid, true);
    }




    @Override
    public void deleteTuple(TransactionId tid, Tuple t) throws TransactionAbortedException {
        PageId tuplePid = t.getRecordId().getPageId();
        if (tuplePid.getTableId() != tableId
                || tuplePid.pageNumber() >= numPages
                || tuplePid.pageNumber() < 0) {
            throw new DbException("Tuple " + t + "is not in files");
        }
        SlottedPage page = (SlottedPage)bm.pinPage(tuplePid, pageMaker);
        page.deleteTuple(t);
        bm.unpinPage(tuplePid, true);
        t.setRecordId(null);
    }

    @Override
    public DbFileIterator iterator(TransactionId tid) {
        return new HeapFileIterator(tid);
    }

    /**
     * @see DbFileIterator
     */
    private class HeapFileIterator implements DbFileIterator {
        private int currPageNo;
        private Iterator<Tuple> pageIterator;
        private boolean isOpen;

        public HeapFileIterator(TransactionId tid) {
            currPageNo = 0;
        }

        @Override
        public void open() throws TransactionAbortedException {
            if (currPageNo < numPages) {
                SimplePageId pid = new SimplePageId(tableId, currPageNo);
                pageIterator = ((SlottedPage) bm.pinPage(pid, pageMaker)).iterator();
                bm.unpinPage(pid, false);
            }
            isOpen = true;
        }

        /**
         * @return the page number of the next page that has empty slots. if all slots are full, return numPages.
         */

        private int nextNonFullPageNo() {
            int pageNo = currPageNo+1;
            SimplePageId nextPageId;
            SlottedPage page;
            boolean foundPage = false;
            while (pageNo < numPages) {
                nextPageId = new SimplePageId(tableId, pageNo);
                page = (SlottedPage) bm.pinPage(nextPageId, pageMaker);
                if (page.iterator().hasNext()) {
                    foundPage = true;
                }
                bm.unpinPage(nextPageId, false);
                if (foundPage) {
                    break;
                }
                pageNo++;
            }
            return pageNo;
        }

        @Override
        public boolean hasNext() throws TransactionAbortedException {
            if (!isOpen) {
                return false;
            }
            if (pageIterator == null) {
                return false;
            }
            if (pageIterator.hasNext()) { //if current page has next tuple, return true
                return true;
            }
            //else check if following pages have available tuples
            currPageNo = nextNonFullPageNo();
            if(currPageNo >= 0 && currPageNo < numPages) {
                SimplePageId pid = new SimplePageId(tableId, currPageNo);
                pageIterator = ((SlottedPage) bm.pinPage(pid, pageMaker)).iterator();
                bm.unpinPage(pid, false);
                return true;
            }
            else {
                return false;
            }
        }

        @Override
        public Tuple next() throws TransactionAbortedException, NoSuchElementException {
            if (!isOpen) {
                return null;
            }
            if (!hasNext()) {
                throw new NoSuchElementException();
            }
            return pageIterator.next();
        }

        @Override
        public void rewind() throws TransactionAbortedException {
            currPageNo = 0;
            open();
        }

        @Override
        public void close() {
            isOpen = false;
        }
    }

}
