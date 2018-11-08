package colgatedb.operators;

import colgatedb.DbException;
import colgatedb.transactions.TransactionAbortedException;
import colgatedb.tuple.Tuple;
import colgatedb.tuple.TupleDesc;

import java.util.ArrayList;
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
 * Filter is an operator that implements a relational select.
 */
public class Filter extends Operator {

    private static final long serialVersionUID = 1L;
    private boolean open;
    private DbIterator child;
    private Predicate p;
    private Tuple current;
    private TupleDesc td;
    /**
     * Constructor accepts a predicate to apply and a child operator to read
     * tuples to filter from.
     *
     * @param p     The predicate to filter tuples with
     * @param child The child operator
     */
    public Filter(Predicate p, DbIterator child) {
        this.p = p;
        this.child = child;
        this.td = child.getTupleDesc();
    }

    public Predicate getPredicate() {
        return this.p;
    }

    public TupleDesc getTupleDesc() {
        return td;
    }

    @Override
    public void open() throws DbException, NoSuchElementException,
            TransactionAbortedException {
        this.open = true;
        child.open();
    }

    @Override
    public void close() {
        open = false;
        this.child.close();
    }

    @Override
    public void rewind() throws DbException, TransactionAbortedException {
        this.child.rewind();
    }

    @Override
    public boolean hasNext() throws DbException, TransactionAbortedException {
        if(!open){
            return false;
        }
        if(current != null){
            return true;
        }
        while (child.hasNext()){
            current = child.next();
            if(p.filter(current)){
                return true;
            }
        }
        current = null;
        return false;
    }

    @Override
    public Tuple next() throws DbException, TransactionAbortedException,
            NoSuchElementException {
        if (!hasNext()) {
            throw new NoSuchElementException("no more tuples!");
        }
        Tuple t = current;
        current = null;
        return t;
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
