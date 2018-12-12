package colgatedb.logging;

import colgatedb.BufferManager;
import colgatedb.Database;
import colgatedb.page.Page;
import colgatedb.page.PageId;
import colgatedb.page.SlottedPage;
import colgatedb.transactions.Transaction;
import colgatedb.transactions.TransactionId;

import javax.xml.crypto.Data;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.HashSet;
import java.util.Set;

import static colgatedb.logging.LogFileImpl.LONG_SIZE;
import static colgatedb.logging.LogFileImpl.NO_CHECKPOINT_ID;

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
public class LogFileRecovery {

    private final RandomAccessFile readOnlyLog;

    /**
     * Helper class for LogFile during rollback and recovery.
     * This class given a read only view of the actual log file.
     *
     * If this class wants to modify the log, it should do something
     * like this:  Database.getLogFile().logAbort(tid);
     *
     * @param readOnlyLog a read only copy of the log file
     */
    public LogFileRecovery(RandomAccessFile readOnlyLog) {
        this.readOnlyLog = readOnlyLog;
    }

    /**
     * Print out a human readable representation of the log
     */
    public void print() throws IOException {
        System.out.println("-------------- PRINT OF LOG FILE -------------- ");
        // since we don't know when print will be called, we can save our current location in the file
        // and then jump back to it after printing
        Long currentOffset = readOnlyLog.getFilePointer();

        readOnlyLog.seek(0);
        long lastCheckpoint = readOnlyLog.readLong(); // ignore this
        System.out.println("BEGIN LOG FILE");
        while (readOnlyLog.getFilePointer() < readOnlyLog.length()) {
            int type = readOnlyLog.readInt();
            long tid = readOnlyLog.readLong();
            switch (type) {
                case LogType.BEGIN_RECORD:
                    System.out.println("<T_" + tid + " BEGIN>");
                    break;
                case LogType.COMMIT_RECORD:
                    System.out.println("<T_" + tid + " COMMIT>");
                    break;
                case LogType.ABORT_RECORD:
                    System.out.println("<T_" + tid + " ABORT>");
                    break;
                case LogType.UPDATE_RECORD:
                    Page beforeImg = LogFileImpl.readPageData(readOnlyLog);
                    Page afterImg = LogFileImpl.readPageData(readOnlyLog);  // after image
                    System.out.println("<T_" + tid + " UPDATE pid=" + beforeImg.getId() +">");
                    break;
                case LogType.CLR_RECORD:
                    afterImg = LogFileImpl.readPageData(readOnlyLog);  // after image
                    System.out.println("<T_" + tid + " CLR pid=" + afterImg.getId() +">");
                    break;
                case LogType.CHECKPOINT_RECORD:
                    int count = readOnlyLog.readInt();
                    Set<Long> tids = new HashSet<Long>();
                    for (int i = 0; i < count; i++) {
                        long nextTid = readOnlyLog.readLong();
                        tids.add(nextTid);
                    }
                    System.out.println("<T_" + tid + " CHECKPOINT " + tids + ">");
                    break;
                default:
                    throw new RuntimeException("Unexpected type!  Type = " + type);
            }
            long startOfRecord = readOnlyLog.readLong();   // ignored, only useful when going backwards thru log
        }
        System.out.println("END LOG FILE");

        // return the file pointer to its original position
        readOnlyLog.seek(currentOffset);

    }

    /**
     * Undo page write of given tid
     * @param tid
     * @throws IOException
     */
    private void undoUpdate(long tid) throws IOException{
        Page beforeImg = LogFileImpl.readPageData(readOnlyLog);
        LogFileImpl.readPageData(readOnlyLog);
        Database.getDiskManager().writePage(beforeImg);
        Database.getLogFile().logCLR(tid, beforeImg);
        BufferManager bm = Database.getBufferManager();
        PageId pid = beforeImg.getId();
        if(bm.isDirty(pid)) {
            bm.discardPage(pid);
        }
    }

    /**
     * Rollback the specified transaction, setting the state of any
     * of pages it updated to their pre-updated state.  To preserve
     * transaction semantics, this should not be called on
     * transactions that have already committed (though this may not
     * be enforced by this method.)
     *
     * This is called from LogFile.recover after both the LogFile and
     * the BufferPool are locked.
     *
     * @param tidToRollback The transaction to rollback
     * @throws java.io.IOException if tidToRollback has already committed
     */
    public void rollback(TransactionId tidToRollback) throws IOException {
         readOnlyLog.seek(readOnlyLog.length() - LONG_SIZE);
         long recordStart;
         while (readOnlyLog.getFilePointer() > 0) {
             recordStart = readOnlyLog.readLong();
             readOnlyLog.seek(recordStart);
             int type = readOnlyLog.readInt();
             long tid = readOnlyLog.readLong();
             if(tidToRollback.getId() == tid) {
                 if (type == LogType.UPDATE_RECORD) {
                     undoUpdate(tid);
                 }
                 else if(type == LogType.BEGIN_RECORD) {
                     Database.getLogFile().logAbort(tidToRollback.getId());
                 }
                 else if(type == LogType.COMMIT_RECORD){
                     throw new IOException("Transaction " + tid + " has already been commited!");
                 }
             }
             readOnlyLog.seek(recordStart - LONG_SIZE);
         }
    }

    /**
     * Recover the database system by ensuring that the updates of
     * committed transactions are installed and that the
     * updates of uncommitted transactions are not installed.
     *
     * This is called from LogFile.recover after both the LogFile and
     * the BufferPool are locked.
     */
    public void recover() throws IOException {
        Set<Long> losers = new HashSet<>();
        //read last checkpoint
        readOnlyLog.seek(0);
         long lastCheckPoint = readOnlyLog.readLong();
         //populate losers
         if(lastCheckPoint != NO_CHECKPOINT_ID){
             readOnlyLog.seek(lastCheckPoint);
             if(readOnlyLog.readInt() == LogType.CHECKPOINT_RECORD) {
                 readOnlyLog.skipBytes(LogFileImpl.LONG_SIZE);
                 int numActive = readOnlyLog.readInt();
                 for (int i = 0; i < numActive; i++) {
                     losers.add(readOnlyLog.readLong());
                 }
             }
             readOnlyLog.skipBytes(LogFileImpl.LONG_SIZE);
         }
         //REDO
         long recordStart = readOnlyLog.getFilePointer();
         while (recordStart < readOnlyLog.length()){
             int type = readOnlyLog.readInt();
             long tid = readOnlyLog.readLong();
             if(type == LogType.BEGIN_RECORD) {
                 losers.add(tid);
             }
             else if(type == LogType.COMMIT_RECORD || type == LogType.ABORT_RECORD) {
                 losers.remove(tid);
             }
             else if(type == LogType.UPDATE_RECORD) {
                LogFileImpl.readPageData(readOnlyLog);
                Page afterImage = LogFileImpl.readPageData(readOnlyLog);
                Database.getDiskManager().writePage(afterImage);

             }
             else if(type == LogType.CLR_RECORD) {
                 Page afterImage = LogFileImpl.readPageData(readOnlyLog);
                 Database.getDiskManager().writePage(afterImage);
             }
             readOnlyLog.skipBytes(LogFileImpl.LONG_SIZE);
             recordStart = readOnlyLog.getFilePointer();
         }
         //UNDO
        readOnlyLog.seek(readOnlyLog.length() - LONG_SIZE);

        while (readOnlyLog.getFilePointer() > 0) {
            recordStart = readOnlyLog.readLong();
            readOnlyLog.seek(recordStart);
            int type = readOnlyLog.readInt();
            long tid = readOnlyLog.readLong();
            if(losers.contains(tid)) {  //if this txn is a loser
                if (type == LogType.UPDATE_RECORD) {
                    undoUpdate(tid);
                }
                else if(type == LogType.BEGIN_RECORD) {
                    Database.getLogFile().logAbort(tid);
                    losers.remove(tid);
                    if(losers.isEmpty()) {
                        break;
                    }
                }
            }
            readOnlyLog.seek(recordStart - LONG_SIZE);
        }
    }
}
