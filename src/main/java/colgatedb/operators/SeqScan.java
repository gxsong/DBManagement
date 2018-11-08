package colgatedb.operators;

import colgatedb.Database;
import colgatedb.DbException;
import colgatedb.dbfile.DbFileIterator;
import colgatedb.transactions.TransactionAbortedException;
import colgatedb.transactions.TransactionId;
import colgatedb.tuple.Tuple;
import colgatedb.tuple.TupleDesc;
import colgatedb.tuple.Type;

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
 * The contents of this file are taken almost verbatim from the SimpleDB project.
 * We are grateful for Sam's permission to use and adapt his materials.
 */

/**
 * SeqScan is an implementation of a sequential scan access method that reads
 * each tuple of a table in no particular order (e.g., as they are laid out on
 * disk).
 */
public class SeqScan implements DbIterator {

    private TransactionId tid;
    private int tableid;
    private String tableAlias;
    private TupleDesc td;
    private boolean isOpen;
    private DbFileIterator dbFileIterator;

    /**
     * Creates a sequential scan over the specified table as a part of the
     * specified transaction.
     *
     * @param tid        The transaction this scan is running as a part of.
     * @param tableid    the id of the table to scan.  The actual table can be retrieved from the Catalog.
     * @param tableAlias the alias of this table (needed by the parser); the returned
     *                   tupleDesc should have fields with name tableAlias.fieldName
     *                   (note: this class is not responsible for handling a case where
     *                   tableAlias or fieldName are null. It shouldn't crash if they
     *                   are, but the resulting name can be null.fieldName,
     *                   tableAlias.null, or null.null).
     */
    public SeqScan(TransactionId tid, int tableid, String tableAlias) {
        this.tid = tid;
        this.tableid = tableid;
        if(tableAlias == null){
            this.tableAlias = "null";
        }
        else {
            this.tableAlias = tableAlias;
        }
        initTd();
        this.dbFileIterator = Database.getCatalog().getDatabaseFile(tableid).iterator(tid);
    }

    /**makes a new TupleDesc as this.td out of the table's original TupleDesc
     * each field of this.td has field name tableAlias.fieldName
     */
    private void initTd(){
        TupleDesc originalTd = Database.getCatalog().getTupleDesc(tableid);
        int numFields = originalTd.numFields();
        Type[] typeArr = new Type[numFields];
        String[] fieldArr = new String[numFields];
        StringBuilder fieldName;
        for (int i = 0; i < numFields; i++){
            fieldName = new StringBuilder();
            String originalFieldName = originalTd.getFieldName(i);
            if(originalFieldName == null){
                originalFieldName = "null";
            }
            fieldName.append(tableAlias).append(".").append(originalFieldName);
            typeArr[i] = originalTd.getFieldType(i);
            fieldArr[i] = fieldName.toString();
            fieldName.setLength(0);
        }
        this.td = new TupleDesc(typeArr, fieldArr);
    }

    public SeqScan(TransactionId tid, int tableid) {
        this(tid, tableid, Database.getCatalog().getTableName(tableid));
    }

    /**
     * @return return the table name of the table the operator scans. This should
     * be the actual name of the table in the catalog of the database
     */
    public String getTableName() {
        return Database.getCatalog().getTableName(this.tableid);
    }

    /**
     * @return Return the alias of the table this operator scans.
     */
    public String getAlias() {
        return this.tableAlias;
    }

    public void open() throws DbException, TransactionAbortedException {
        this.isOpen = true;
        this.dbFileIterator.open();
    }

    /**
     * Returns the TupleDesc with field names from the underlying HeapFile,
     * prefixed with the tableAlias string from the constructor. This prefix
     * becomes useful when joining tables containing a field(s) with the same
     * name.
     *
     * @return the TupleDesc with field names from the underlying HeapFile,
     * prefixed with the tableAlias string from the constructor.
     */
    public TupleDesc getTupleDesc() {
        return this.td;
    }

    public boolean hasNext() throws TransactionAbortedException, DbException {
        if(!isOpen){
            return false;
        }
        if(dbFileIterator == null){
            return false;
        }
        return dbFileIterator.hasNext();
    }

    public Tuple next() throws NoSuchElementException,
            TransactionAbortedException, DbException {
        if(!isOpen){
            return null;
        }
        if(!hasNext()){
            throw new NoSuchElementException();
        }
        return dbFileIterator.next();
    }

    public void close() {
        isOpen = false;
        dbFileIterator.close();
    }

    public void rewind() throws DbException, NoSuchElementException,
            TransactionAbortedException {
        dbFileIterator.rewind();
    }
}
