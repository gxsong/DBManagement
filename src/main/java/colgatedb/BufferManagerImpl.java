package colgatedb;

import colgatedb.page.Page;
import colgatedb.page.PageId;
import colgatedb.page.PageMaker;

import java.util.BitSet;
import java.util.HashMap;
import java.util.Set;

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
public class BufferManagerImpl implements BufferManager {

    private int capacity;
    private int occupancy = 0;
    /**
     * The buffer pool contains a HashMap and a LinkedList (queue).
     * A FrameNode only exists if there is a Page in its Frame.
     * A FrameNode is in the queue only if the pinCount of its frame is 0.
     */
    private HashMap<PageId, FrameNode> map;
    /**
     * When a Page is Unpinned, if its pinCount = 0, its FrameNode is added to the tail of the queue,
     * i.e., the "mru" end.
     * If the page is clean, the clean pointers are also updated.
     *
     * Note that it is a single queue for all qualified FrameNodes,
     * rather than one for dirty nodes and one for clean ones.
     *
     * see implementation of inner class FrameNode.
     */
    private FrameNode lru = null; //head of the queue, points to least recently used FrameNode
    private FrameNode mru = null; //tail of the queue, points to most recently used FrameNode
    private FrameNode cleanLru = null; //"clean head" of the queue
    private FrameNode cleanMru = null; //"clean tail" of the queue

    private boolean allowEvictDirty;

    private DiskManager dm;
    /**
     * Construct a new buffer manager.
     * @param numPages maximum size of the buffer pool
     * @param dm the disk manager to call to read/write pages
     */
    public BufferManagerImpl(int numPages, DiskManager dm) {
       this.capacity = numPages;
       this.dm = dm;
       this.map =  map = new HashMap<>(numPages);
    }

    /**
     * O(1), since eviction takes constant time.
     * @param pid pid of desired page
     * @param pageMaker used to create Page object if it must be read from disk
     * @return
     */
    @Override
    public synchronized Page pinPage(PageId pid, PageMaker pageMaker) {
        FrameNode node = null;
        if(inBufferPool(pid)){
            node = map.get(pid);
            if(isPinCountZero(node)) //can't evict after pinned, remove from queue.
                removeFromQueue(node); //O(1), see javadoc.
            incrementPinCount(node);
        }
        else{
            node = new FrameNode(dm.readPage(pid, pageMaker));
            if(isFull())
                evictPage(); //O(1), see javadoc.
            map.put(pid, node);
            occupancy++;
        }
        return node.frame.page;
    }

    /**
     * O(1).
     * @param pid pid of page to unpin
     * @param isDirty whether or not the user of this page dirtied it
     */
    @Override
    public synchronized void unpinPage(PageId pid, boolean isDirty) {
        if(!inBufferPool(pid))
            throw new BufferManagerException("Page Doesn't Present!");

        FrameNode node = map.get(pid);

        if(isPinCountZero(node))
            throw new BufferManagerException("Page is not pinned!");

        decrementPinCount(node);

        if(isDirty && !node.frame.isDirty)
            node.frame.isDirty = true;

        if(isPinCountZero(node)) //candidate for eviction, add to queue.
            addToQueue(node); //O(1)
    }

    /**
     * Evict page when buffer pool is full and a new page needs to be inserted.
     * If allowEvictDirty, flush (if dirty) and evict the head of queue.
     * Otherwise, evict the clean head of queue.
     *
     * O(1), since getting the head/clean head is O(1),
     * and removing and flushing are both O(1).
     */
    private void evictPage() {
        Page page = null;
        FrameNode nodeToEvict = null;
        if(!allowEvictDirty) {
            if(cleanLru == null)
                throw new BufferManagerException("No Page To Evict!");
            nodeToEvict = cleanLru;
        }
        else {
            if (lru == null)
                throw new BufferManagerException("No Page To Evict!");
            nodeToEvict = lru;
            flushPage(nodeToEvict.pid); //takes O(1), see comments in implementation.
        }
        discardPage(nodeToEvict.pid); //wraps removeNodeFromQueue() is called, which takes O(1).
    }

    @Override
    public synchronized void flushPage(PageId pid) {
        if(!inBufferPool(pid))
            throw new BufferManagerException("Page Doesn't Present!");
        FrameNode node = map.get(pid);
        if(isDirty(pid)) {
            dm.writePage(node.frame.page);
            node.frame.isDirty = false;
            /*
             If the page flushed has pinCount = 0, move it to the tail of the queue.
             It will break the rule that the the queue is ordered by reference time,
             but removing and adding are constant time.
             Otherwise, it will be linear time to update the clean pointers,
             since we do not know if this node is before or after the clean head.
             */
            if (isPinCountZero(node)) {
                removeFromQueue(node); //O(1). see javadoc.
                addToQueue(node); //O(1). see javadoc.
            }
        }
    }

    @Override
    public synchronized void flushAllPages() {
        Set<PageId> pids = map.keySet();
        for (PageId pid: pids){
            flushPage(pid);
        }
    }

    @Override
    public synchronized void evictDirty(boolean allowEvictDirty) {
        this.allowEvictDirty = allowEvictDirty;
    }

    @Override
    public synchronized void allocatePage(PageId pid) {
        dm.allocatePage(pid);
    }

    @Override
    public synchronized boolean isDirty(PageId pid) {
        if(!inBufferPool(pid))
            return false;
        return map.get(pid).frame.isDirty;
    }

    @Override
    public synchronized boolean inBufferPool(PageId pid) {
        return map.containsKey(pid);
    }

    @Override
    public synchronized Page getPage(PageId pid) {
        if(!inBufferPool(pid))
            throw new BufferManagerException("Page doesn't Present!");
        return map.get(pid).frame.page;
    }

    @Override
    public synchronized void discardPage(PageId pid) {
        if(!inBufferPool(pid))
            return;
        FrameNode node = map.get(pid);
        removeFromQueue(node);
        map.remove(pid);
        occupancy--;
    }

    private boolean isFull() {
        return this.occupancy == this.capacity;
    }

    /**
     * Remove given node from queue.
     * O(1), since no iteration needed.
     * @param node node to be removed.
     */
    private void removeFromQueue(FrameNode node) {
        if(!node.frame.isDirty) {
            if (node.cleanPrev != null)
                node.cleanPrev.next = node.cleanNext;
            if (node.cleanNext != null)
                node.cleanNext.prev = node.cleanPrev;
            if(node == cleanLru)
                cleanLru = node.cleanNext;
            if(node == cleanMru)
                cleanMru = node.cleanPrev;
        }
        if(node.prev != null)
            node.prev.next = node.next;
        if(node.next != null)
            node.next.prev = node.prev;
        if(node == lru)
            lru = node.next;
        if(node == mru)
            mru = node.prev;
    }

    /**
     * Add node to queue.
     * O(1), since no iteration through the queue is needed.
     * @param node node to be added.
     */
    private void addToQueue(FrameNode node){
        if(mru == null)
            mru = node;
        else {
            mru.next = node;
            node.prev = mru;
            mru = mru.next;
        }
        if(lru == null)
            lru = node;
        if(!node.frame.isDirty) {
            if(cleanMru == null)
                cleanMru = node;
            else {
                cleanMru.next = node;
                node.prev = cleanMru;
                cleanMru = cleanMru.next;
            }
            if(cleanLru == null)
                cleanLru = node;
        }
    }

    private boolean isPinCountZero(FrameNode node) {
        return node.frame.pinCount == 0;
    }

    private void incrementPinCount(FrameNode node) {
        node.frame.pinCount++;
    }

    private void decrementPinCount(FrameNode node) {
        node.frame.pinCount--;
    }

    /**
     * A FrameNode contains a frame and four pointers. It uses the pid of the page in its frame as a key.
     * The "next" of a FrameNode is inserted after itself.
     * The "prev" of a FrameNode is inserted before itself.
     * If the page in FrameNode is clean, its cleanPrev and cleanNext pointers
     * will point to the prev and next clean nodes in the queue, if present.
     */
    private class FrameNode {
        private PageId pid;
        private Frame frame;
        private FrameNode prev = null;
        private FrameNode next = null;
        private FrameNode cleanPrev = null;
        private FrameNode cleanNext = null;

        private FrameNode(Page page){
            this.pid = page.getId();
            this.frame = new Frame(page);
        }
    }


    /**
     * A frame holds one page and maintains state about that page.  You are encouraged to use this
     * in your design of a BufferManager.  You may also make any warranted modifications.
     */
    private class Frame {
        private Page page;
        private int pinCount;
        private boolean isDirty;

        private Frame(Page page) {
            this.page = page;
            this.pinCount = 1;   // assumes Frame is created on first pin -- feel free to modify as you see fit
            this.isDirty = false;
        }
    }

}