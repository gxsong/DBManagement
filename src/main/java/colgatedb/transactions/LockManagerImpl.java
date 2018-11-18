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
    private Graph graph;

    public LockManagerImpl() {
        lockTable = new HashMap<>();
        graph = new Graph();
    }

    @Override
    public void acquireLock(TransactionId tid, PageId pid, Permissions perm) throws TransactionAbortedException {
        boolean waiting = true;
        boolean grant = false;
        synchronized (this) {
        LockTableEntry tableEntry = lockTable.get(pid);
        if(tableEntry == null){
            tableEntry = new LockTableEntry();
            lockTable.put(pid, tableEntry);
        }
        if(!holdsLock(tid, pid, perm)) { //if this transcation does not hold requested lock on this object
            tableEntry.addToQueue(tid, perm, holdsLock(tid, pid, READ_ONLY) && perm == READ_WRITE);
            while (waiting) {
                    if(perm == READ_ONLY){ //if requesting shared lock
                        if(tableEntry.isExclusive() && !holdsLock(tid, pid, READ_ONLY)){ //if this object is not exclusively held
                            TransactionId holder = tableEntry.lockHolders.iterator().next();
                            if (deadlockPrevention(tid, holder)) { //abort using wait-die if deadlock detected
                                tableEntry.pollFromQueue();
                                throw new TransactionAbortedException();
                            }
                        }
                        else grant = true;
                    }

                    else { //if requesting exclusive lock
                        if(tableEntry.isUsed()){ //if this object is locked
                            if(tableEntry.lockHolders.size() == 1 && holdsLock(tid, pid, READ_ONLY)){ //if it is an upgrade request and nobody else is holding this lock
                                grant = true;
                            }
                            else {
                                //if deadlock detected, abort using wait-die
                                for (TransactionId holder : tableEntry.lockHolders) {
                                    graph.addEdge(tid, holder);
                                    if (deadlockPrevention(tid, holder)) {
                                        tableEntry.pollFromQueue();
                                        throw new TransactionAbortedException();
                                    }
                                }
                            }
                        }
                        else if(tableEntry.atFront(tid, perm)){ //if this request is at the front of queue
                            grant = true;
                        }
                    }
                    if(grant){
                        if (perm == Permissions.READ_WRITE) {  //clear other holders if this txn is granted exclusive lock
                            tableEntry.lockHolders.clear();
                        }
                        //update locktable entry
                        tableEntry.addHolder(tid);
                        tableEntry.setLockType(perm);
                        tableEntry.pollFromQueue();
                        graph.addNode(tid);
                        for(TransactionId older: tableEntry.lockHolders){
                            graph.removeEdge(tid, older);
                        }
                        waiting = false;
                    }
                    else {
                        try {
                            wait();
                        } catch (InterruptedException e) { }
                    }
                }
            }
        }

    }

    /**
     * add new edge to graph, detect cycle, abort using wait-die
     * @param tid transaction issuing request
     * @param holder lock holder
     * @return true if current transaction is aborted
     */
    private synchronized boolean deadlockPrevention(TransactionId tid, TransactionId holder){
        graph.addEdge(tid, holder);
        if(graph.hasCycle()){
            if(!graph.isOlder(tid, holder)){
                graph.removeNode(tid);
                return true;
            }
        }
        return false;
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
            graph.removeNode(tid);
            notifyAll();
        }

        else{
            throw new LockManagerException(tid + " does not hold lock on " + pid);
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

    static class Graph{
        HashMap<TransactionId, Node> nodeMap;
        HashMap<TransactionId, Set<TransactionId>> adjList;
        int timestamp;

        Graph (){
            this.nodeMap = new HashMap<>();
            this.adjList = new HashMap<>();
            timestamp = 0;
        }

        void addNode(TransactionId tid){
            if(nodeMap.putIfAbsent(tid, new Node(tid, timestamp)) == null){
                timestamp++;
            }
        }

        void addEdge(TransactionId tid1, TransactionId tid2) {
            addNode(tid1);
            addNode(tid2);
            if(adjList.putIfAbsent(tid1, new HashSet<>(Arrays.asList(tid2))) == null){
                adjList.get(tid1).add(tid2);
            }
        }

        void removeEdge(TransactionId tid1, TransactionId tid2){
            if(nodeMap.containsKey(tid1) && nodeMap.containsKey(tid2)){
                if(adjList.containsKey(tid1)){
                    adjList.get(tid1).remove(tid2);
                }
            }

        }

        void removeNode(TransactionId tid){
            if(nodeMap.containsKey(tid)){
                for (Set<TransactionId> nodes: adjList.values()){
                    nodes.remove(tid);
                }
                adjList.remove(tid);
                nodeMap.remove(tid);
            }
        }

        void recolor(){
            for(Node n: nodeMap.values()){
                n.color = 0;
            }
        }

        boolean hasCycle(){
            recolor();
            Node n;
            for(TransactionId tid: adjList.keySet()){
                n = nodeMap.get(tid);
                if(n.color == 0){
                    if(dfs(n)) return true;
                }
            }
            return false;
        }

        boolean dfs(Node node){
            Node c;
            Set<TransactionId> children = adjList.get(node.tid);
            if(children != null) {
                node.color = 1;
                for (TransactionId tid : children) {
                    c = nodeMap.get(tid);
                    if (c.color == 0) {
                        if(dfs(c)){
                            return true;
                        }
                    }
                    else if (c.color == 1){
                        return true;
                    }
                }
            }
            node.color = -1;
            return false;
        }

        boolean isOlder(TransactionId tid1, TransactionId tid2) {
            return (nodeMap.get(tid1).ts <= nodeMap.get(tid2).ts);
        }


        class Node {
            TransactionId tid;
            int ts;
            int color; //-1 = black, 0 = white, 1 = grey
            Node (TransactionId tid, int ts){
                this.tid = tid;
                this.ts = ts;
                this.color = 0;
            }
        }
        class Edge {
            Node s;
            Node t;
            Edge (Node s, Node t){
                this.s = s;
                this.t = t;
            }
        }
    }

}
