package colgatedb.transactions;

import colgatedb.page.PageId;

import java.util.*;

import static colgatedb.transactions.Permissions.READ_ONLY;
import static colgatedb.transactions.Permissions.READ_WRITE;

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
public class LockManagerImpl implements LockManager {
    private HashMap<PageId, LockTableEntry> lockTable;


    public LockManagerImpl() {
        lockTable = new HashMap<>();
    }

    @Override
    public void acquireLock(TransactionId tid, PageId pid, Permissions perm) throws TransactionAbortedException {
        boolean waiting = true;
        boolean inqueue = false;
        if(!holdsLock(tid, pid, perm)) { //this transcation does not hold lock on this object
            while (waiting) {
                synchronized (this) {
                    LockTableEntry tableEntry = lockTable.get(pid);
                    if(tableEntry == null){
                        tableEntry = new LockTableEntry();
                        lockTable.put(pid, tableEntry);
                    }

                    if ((perm == READ_ONLY           //if requesting shared lock, should grant lock if
                            && !tableEntry.isExclusive()) // this lock is not exclusively used

                        ||(perm == Permissions.READ_WRITE   //if requesting exclusive, should grant lock if
                            && ((!tableEntry.isUsed() && (tableEntry.atFront(tid, perm)) //the lock is unused and this request is at front of queue
                                || holdsLock(tid, pid, READ_ONLY))))){ // or if it is an upgrade request

                        if (perm == Permissions.READ_WRITE) {  //clear other holders if this txn is granted exclusive lock
                            tableEntry.lockHolders.clear();
                        }
                        //update locktable entry
                        tableEntry.addHolder(tid);
                        tableEntry.setLockType(perm);
                        tableEntry.pollFromQueue();
                        waiting = false;
                    }
                    else {
                        if(!inqueue) {
                            tableEntry.addToQueue(tid, perm, holdsLock(tid, pid, READ_ONLY)&&perm == READ_WRITE);
                            inqueue = true;
                        }
                        try {
                            wait();
                        } catch (InterruptedException e) {}
                    }
                }
            }
        }

    }

    @Override
    public synchronized boolean holdsLock(TransactionId tid, PageId pid, Permissions perm) {
        LockTableEntry tableEntry = lockTable.get(pid);
        return tableEntry!= null
                && tableEntry.lockHolders.contains(tid)
                && tableEntry.lockAsStrongAs(perm);
    }

    @Override
    public synchronized void releaseLock(TransactionId tid, PageId pid) {
        LockTableEntry tableEntry = lockTable.get(pid);
        if(tableEntry != null && holdsLock(tid, pid, READ_ONLY)){
            tableEntry.lockHolders.remove(tid);
            //update lock type
            if(!tableEntry.isUsed()){
                tableEntry.setLockType(null);
            }
            else {
                tableEntry.setLockType(READ_ONLY);
            }
            notifyAll();
        }

        else{
            throw new LockManagerException("lock is not held!!");
        }
    }

    @Override
    public synchronized List<PageId> getPagesForTid(TransactionId tid) {
        List<PageId> res = new ArrayList<>();
        for(PageId pid: lockTable.keySet()){
            if(holdsLock(tid, pid, READ_ONLY)){
                res.add(pid);
            }
        }
        return res;
    }

    @Override
    public synchronized List<TransactionId> getTidsForPage(PageId pid) {
        LockTableEntry tableEntry = lockTable.get(pid);
        if(tableEntry != null){
            return new ArrayList<>(tableEntry.lockHolders);
        }
        return null;
    }

    class LockTableEntry {

        // some suggested private instance variables; feel free to modify
        private Permissions lockType;             // null if no one currently has a lock
        private Set<TransactionId> lockHolders;   // a set of txns currently holding a lock on this page
        private Deque<LockRequest> requests;       // a queue of outstanding requests

        private LockTableEntry() {
            lockType = null;
            lockHolders = new HashSet<>();
            requests = new LinkedList<>();
            // you may wish to add statements here.
        }

        private boolean lockAsStrongAs(Permissions perm){
            return lockType != null && this.lockType.permLevel >= perm.permLevel;
        }

        private boolean isExclusive(){
            return lockType == Permissions.READ_WRITE;
        }

        public boolean isUsed(){
            return !lockHolders.isEmpty();
        }

        public void pollFromQueue(){
            requests.pollFirst();
        }

        private boolean atFront(TransactionId tid, Permissions perm){
            if(requests.isEmpty()){
                requests.offerFirst(new LockRequest(tid, perm));
                return true;
            }
            LockRequest head = requests.peekFirst();
            return head.tid.equals(tid) && head.perm == perm;
        }

        private void addHolder(TransactionId tid){
            lockHolders.add(tid);
        }
        private void setLockType(Permissions perm){
            this.lockType = perm;
        }

        private void addToQueue(TransactionId tid, Permissions perm, boolean isUpgrade){
            LockRequest request = new LockRequest(tid, perm);
            if(isUpgrade){
                requests.addFirst(request);
            }
            else{
                requests.addLast(request);
            }
        }

        /**
         * A class representing a single lock request.  Simply tracks the txn and the desired lock type.
         * Feel free to use this, modify it, or not use it at all.
         */
        class LockRequest {
            public final TransactionId tid;
            private final Permissions perm;

            private LockRequest(TransactionId tid, Permissions perm) {
                this.tid = tid;
                this.perm = perm;
            }

            public boolean equals(Object o) {
                if (!(o instanceof LockRequest)) {
                    return false;
                }
                LockRequest otherLockRequest = (LockRequest) o;
                return tid.equals(otherLockRequest.tid) && perm.equals(otherLockRequest.perm);
            }

            public String toString() {
                return "Request[" + tid + "," + perm + "]";
            }

        }
    }
}
