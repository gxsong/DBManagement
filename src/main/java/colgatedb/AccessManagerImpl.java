package colgatedb;

import colgatedb.page.Page;
import colgatedb.page.PageId;
import colgatedb.page.PageMaker;
import colgatedb.transactions.*;

import java.io.IOException;
import java.util.*;

/**
 * ColgateDB
 *
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
public class AccessManagerImpl implements AccessManager {

    private boolean force = true;  // indicates whether force policy should be used
    private final BufferManager bm;
    private final LockManager lm;
    private Map<PageId, pinEntry> pinMap;


    /**
     * Initialize the AccessManager, which includes creating a new LockManager.
     * @param bm buffer manager through which all page requests should be made
     */
    public AccessManagerImpl(BufferManager bm) {
        this.bm = bm;
        //bm.evictDirty(false); --allow steal
        this.lm = new LockManagerImpl();
        this.pinMap = new HashMap<>();
    }

    @Override
    public void acquireLock(TransactionId tid, PageId pid, Permissions perm) throws TransactionAbortedException {
        lm.acquireLock(tid, pid, perm);
    }

    @Override
    public boolean holdsLock(TransactionId tid, PageId pid, Permissions perm) {
        return lm.holdsLock(tid, pid, perm);
    }

    @Override
    public void releaseLock(TransactionId tid, PageId pid) {
        if(!pinMap.containsKey(pid) || !pinMap.get(pid).isPinnedBy(tid)){
            lm.releaseLock(tid, pid);
        }
    }

    @Override
    public Page pinPage(TransactionId tid, PageId pid, PageMaker pageMaker) {
        Page page;
        synchronized (this) {
            pinMap.putIfAbsent(pid, new pinEntry(pid));
            pinMap.get(pid).pinUpdate(tid);
            page = bm.pinPage(pid, pageMaker);
        }
        return page;
    }

    @Override
    public void unpinPage(TransactionId tid, Page page, boolean isDirty) {
        synchronized (this) {
            PageId pid = page.getId();
            pinMap.get(pid).unpinUpdate(tid, isDirty);
            bm.unpinPage(pid, isDirty);
            if(isDirty) {
                Database.getLogFile().logWrite(tid, page.getBeforeImage(), page);
                if(force){
                    Database.getLogFile().force();
                }
            }
        }
    }

    @Override
    public void  allocatePage(PageId pid) {
        synchronized (this) {
            bm.allocatePage(pid);
        }
    }

    @Override
    public void transactionComplete(TransactionId tid) {
        transactionComplete(tid, true);
    }

    @Override
    public void transactionComplete(TransactionId tid, boolean commit) {
        if(force) {
            Database.getLogFile().force();
        }
        for(PageId pid: pinMap.keySet()){
            pinEntry entry = pinMap.get(pid);
            int pinCount = entry.tidCountMap.get(tid);
            if (pinMap.get(pid).removeTid(tid)) { //if page is dirtied by this transaction
                if (commit) {
                    bm.flushPage(pid);
                    bm.getPage(pid).setBeforeImage();
                }
                else {
                    bm.discardPage(pid);
                }
            }
            for(int i = 0; i < pinCount; i++){
                bm.unpinPage(pid, false);
            }
        }
        for(PageId pid: lm.getPagesForTid(tid)){
            lm.releaseLock(tid, pid);
        }
    }

    class pinEntry {
        PageId pid;
        HashMap<TransactionId, Integer> tidCountMap;
        HashMap<TransactionId, Boolean> tidDirtyMap;

        pinEntry(PageId pid){
            this.pid = pid;
            this.tidCountMap = new HashMap<>();
            this.tidDirtyMap = new HashMap<>();
        }

        void pinUpdate(TransactionId tid){
            Integer currCount = tidCountMap.putIfAbsent(tid, 1);
            if(currCount != null){
                tidCountMap.put(tid, currCount+1);
            }
            tidDirtyMap.putIfAbsent(tid, false);
        }

        //return true if this page is dirtied by this transaction
        void unpinUpdate(TransactionId tid, boolean dirty){
            Integer newCount = tidCountMap.get(tid)-1;
            tidCountMap.put(tid, newCount);
            if (!tidDirtyMap.get(tid) && dirty) {
                tidDirtyMap.put(tid, true);
            }
        }

        boolean isPinnedBy(TransactionId tid){
            return tidCountMap.containsKey(tid) && tidCountMap.get(tid) > 0;
        }

        boolean removeTid(TransactionId tid){
            tidCountMap.remove(tid);
            boolean dirtied = tidDirtyMap.get(tid);
            tidDirtyMap.remove(tid);
            return dirtied;
        }
    }

    @Override
    public void setForce(boolean force) {
        this.force = force;
    }

}
