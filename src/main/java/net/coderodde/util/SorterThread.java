package net.coderodde.util;

/**
 *
 * @author rodde
 */
final class SorterThread extends Thread {
    
    private final LongBucketSorterInputTask[] tasks;
    
    SorterThread(LongBucketSorterInputTask[] tasks) {
        this.tasks = tasks;
    }
    
    @Override
    public void run() {
        for (LongBucketSorterInputTask task : tasks) {
            parallelSortImplUnsigned()
        }
    }
}
