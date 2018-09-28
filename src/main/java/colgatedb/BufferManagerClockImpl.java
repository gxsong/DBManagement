package colgatedb;

import colgatedb.page.Page;
import colgatedb.page.PageId;
import colgatedb.page.PageMaker;

import java.util.*;

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
public class BufferManagerClockImpl implements BufferManager {

    private boolean allowEvictDirty = false;  // a flag indicating whether a dirty page is candidate for eviction
    private int numPages;
    private DiskManager dm;
    private Frame[] bufferPool;
    private HashMap<PageId, Integer> pidFrameIndMap = new HashMap<>();
    private Clock clock;
    private BitSet frameUsed; // each bit indicates if the frame is occupied by a page. 1 = used, 0 = free
    private int currFreeFrame = 0;
    /**
     * Construct a new buffer manager.
     * @param numPages maximum size of the buffer pool
     * @param dm the disk manager to call to read/write pages
     */
    public BufferManagerClockImpl(int numPages, DiskManager dm) {
        this.numPages = numPages;
        this.dm = dm;
        this.bufferPool = new Frame[numPages];
        this.clock = new Clock(numPages);
        this.frameUsed = new BitSet(numPages);
    }


    @Override
    public synchronized Page pinPage(PageId pid, PageMaker pageMaker) {
        int frameInd = -1;
        if(inBufferPool(pid)) { //if page is in buffer pool, increment pin count
            frameInd = pidFrameIndMap.get(pid);
            bufferPool[frameInd].pinCount++;
        }

        else { //if page is not in buffer pool
            if(isFull()){ //if page is full, use clock to find a page to evict and flush the page to be evicted
                try {
                    frameInd = clock.nextEvictIndex();
                }
                catch (Exception e){
                    throw new BufferManagerException("No page can be Evicted!");
                }

                flushPage(bufferPool[frameInd].page.getId());
            }
            else { //if page is not full, find the next free frame to use
                frameInd = currFreeFrame;
                frameUsed.set(currFreeFrame, true);
                updateNextFreeFrame();
            }
            bufferPool[frameInd] = new Frame(dm.readPage(pid, pageMaker)); //load page
            pidFrameIndMap.put(pid, frameInd);
        }
        //clock.setRefBit(frameInd, true);
        return bufferPool[frameInd].page;

    }

    @Override
    public synchronized void unpinPage(PageId pid, boolean isDirty) {
        if(!inBufferPool(pid))
            throw new BufferManagerException("page " + pid + " is not in Buffer Manager!");
        int frameInd = pidFrameIndMap.get(pid);
        Frame frame = bufferPool[frameInd];

        if(frame.pinCount == 0)
            throw  new BufferManagerException("This page already has pin count = 0!");

        if(!frame.isDirty && isDirty)
            frame.isDirty = isDirty;

        frame.pinCount--;

        if(frame.pinCount == 0)
            clock.setRefBit(frameInd, true);
    }

    @Override
    public synchronized void flushPage(PageId pid) {
        if (inBufferPool(pid)){
            Frame frame = bufferPool[pidFrameIndMap.get(pid)];
            if(frame.isDirty){
                dm.writePage(frame.page);
                frame.isDirty = false;
            }
        }
    }

    @Override
    public synchronized void flushAllPages() {
        for(PageId pid: pidFrameIndMap.keySet()){
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
        return bufferPool[pidFrameIndMap.get(pid)].isDirty;
    }

    @Override
    public synchronized boolean inBufferPool(PageId pid) {
        return pidFrameIndMap.containsKey(pid);
    }

    @Override
    public synchronized Page getPage(PageId pid) {
        if (!inBufferPool(pid))
            throw new BufferManagerException("Page not in Buffer!");
        return bufferPool[pidFrameIndMap.get(pid)].page;
    }

    @Override
    public synchronized void discardPage(PageId pid) {
        if(inBufferPool(pid)){
            int frameInd = pidFrameIndMap.get(pid);
            bufferPool[frameInd] = null;
            frameUsed.set(frameInd, false);
            clock.setRefBit(frameInd, false);
            pidFrameIndMap.remove(pid);
        }
    }

    //helper function that returns true if all frames are used, false otherwise
    private boolean isFull(){
        return frameUsed.cardinality() == numPages;
    }


    //helper function that make "currFreeFrame" the index of the next free frame
    private void updateNextFreeFrame(){
        if(isFull())
            currFreeFrame = -1;
        currFreeFrame = frameUsed.nextClearBit(currFreeFrame+1);
        if(currFreeFrame >= numPages || currFreeFrame < 0)
            currFreeFrame = frameUsed.nextClearBit(0);
    }

    private class Clock {
        private int numPages;
        private BitSet refBits;
        private int curr;

        public Clock(int numPages){
            this.numPages = numPages;
            this.refBits = new BitSet(this.numPages);
            curr = 0;
        }


        public int nextEvictIndex() throws Exception {
            int temp = curr;
            boolean oneCycle = false;
            while (true){
                if(refBits.get(curr))
                    setRefBit(curr, false);
                else{
                    if(isPinCountZero(curr)) {
                        if(allowEvictDirty || !isDirty(curr))
                            return curr;
                    }
                    if(oneCycle)
                        break;
                }
                curr = (++curr) % numPages;

                if(curr == temp)
                    oneCycle = true;
            }

            throw new Exception("No page can be evicted!");
        }

        public void setRefBit(int index, boolean on){
            refBits.set(index, on);
        }

        public boolean isPinCountZero(int index){
            return bufferPool[index].pinCount==0;
        }

        public boolean isDirty(int index){
            return bufferPool[index].isDirty;
        }
    }

    /**
     * A frame holds one page and maintains state about that page.  You are encouraged to use this
     * in your design of a BufferManager.  You may also make any warranted modifications.
     */
    private class Frame {
        private Page page;
        private int pinCount;
        public boolean isDirty;

        public Frame(Page page) {
            this.page = page;
            this.pinCount = 1;   // assumes Frame is created on first pin -- feel free to modify as you see fit
            this.isDirty = false;
        }
    }

}