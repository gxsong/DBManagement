package tutorials.concurrency_tutorial;

public class ZigZagThreads {
    private static final LockManager lm = new LockManager();
    public static LockManager getLockManager() { return lm; }

    public static void main(String args[]) throws InterruptedException {
        int numZigZags = 5;
        for (int i = 0; i < numZigZags; i++) {
            System.out.println("zig " + i);
            new Thread(new Zigger(i)).start();
        }
        for (int i = 0; i < numZigZags; i++) {
            System.out.println("zag " + i);
            new Thread(new Zagger(i)).start();
        }
    }

    static class Zigger implements Runnable {

        protected String myPattern;
        protected boolean isZigger;
        protected int id;

        public Zigger(){}

        public Zigger(int id) {
            myPattern = "//////////";
            isZigger = true;
            this.id = id;
        }

        public void run() {
            getLockManager().acquireLock(isZigger, id);
            System.out.println(myPattern);
            getLockManager().releaseLock(id);
        }
    }

    static class Zagger extends Zigger {
        public Zagger(){}

        public Zagger(int id) {
            myPattern = "\\\\\\\\\\\\\\\\\\\\";
            isZigger = false;
            this.id = id;
        }

    }

    static class LockManager {
        private boolean inUse = false;
        private boolean needZig = true;

        public void acquireLock(boolean isZigger, int id) {
            boolean waiting = true;
            while(waiting) {
                synchronized (this){
                    if (!inUse && isZigger == needZig) {
                        // it's not in use, so we can take it!
                        inUse = true;
                        needZig = !needZig;
                        waiting = false;
                    }

                    else {
                        try {
                            wait();
                        } catch (InterruptedException e) {
                            System.out.println(id + " wake up!!");
                            inUse = true;
                            waiting = false;
                        }
                    }
                }
            }
        }

        public synchronized void releaseLock(int id) {
            inUse = false;
            System.out.println(id + " release");
            notifyAll();
        }
    }}

