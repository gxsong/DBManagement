package colgatedb.operators;

import colgatedb.Database;
import colgatedb.DbException;
import colgatedb.transactions.TransactionAbortedException;
import colgatedb.transactions.TransactionId;
import colgatedb.tuple.IntField;
import colgatedb.tuple.Tuple;
import colgatedb.tuple.TupleDesc;
import colgatedb.tuple.Type;

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
 * The delete operator. Delete reads tuples from its child operator and removes
 * them from the table they belong to.
 */
public class Delete extends Operator {
    private TransactionId tid;
    private DbIterator child;
    private TupleDesc td;
    private boolean open;
    private Tuple resultTuple;
    private boolean firstTime;

    /**
     * Constructor specifying the transaction that this delete belongs to as
     * well as the child to read from.
     *
     * @param t     The transaction this delete runs in
     * @param child The child operator from which to read tuples for deletion
     */
    public Delete(TransactionId t, DbIterator child) {
        this.tid = t;
        this.child = child;
        this.td = new TupleDesc(new Type[]{Type.INT_TYPE}, new String[] {"count"});
        resultTuple = new Tuple(td);
        resultTuple.setField(0, new IntField(0));
        firstTime = true;
    }

    /**
     * @return tuple desc of the insert operator should be a single INT named count
     */
    @Override
    public TupleDesc getTupleDesc() {
        return this.td;
    }

    @Override
    public void open() throws DbException, TransactionAbortedException {
        open = true;
        child.open();
    }

    @Override
    public void close() {
        open = false;
        child.close();
    }

    @Override
    public void rewind() throws DbException, TransactionAbortedException {
        child.rewind();
    }

    /**
     *
     * @return true if this is the first time being called...  even if child is empty,
     *         this iterator still has one tuple to return (the tuple that says that zero
     *         records were deleted).
     */
    @Override
    public boolean hasNext() throws DbException, TransactionAbortedException {
        if(!open){
            return false;
        }
        return firstTime;
    }

    /**
     * Deletes tuples as they are read from the child operator. Deletes are
     * processed via the deleteTuple method on the appropriate DbFile.  The
     * DbFile can be obtained via a combination of the RecordId of the tuple
     * being deleted and the Catalog.
     *
     * @return A single-field tuple containing the number of deleted records.
     * @throws NoSuchElementException if called more than once
     */
    @Override
    public Tuple next() throws DbException, TransactionAbortedException,
            NoSuchElementException {
        if (!hasNext()) {
            throw new NoSuchElementException("no more tuples!");
        }
        int count = 0;
        while (child.hasNext()){
            Tuple t = child.next();
            int tableid = t.getRecordId().getPageId().getTableId();
            Database.getCatalog().getDatabaseFile(tableid).deleteTuple(tid, t);
            count++;
        }
        resultTuple.setField(0, new IntField(count));
        return resultTuple;
    }

    @Override
    public DbIterator[] getChildren() {
        return new DbIterator[]{this.child};
    }

    @Override
    public void setChildren(DbIterator[] children) {
        if (children.length != 1) {
            throw new DbException("Expected only one child!");
        }
        child = children[0];
    }

}
