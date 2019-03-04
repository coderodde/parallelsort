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
//            if (task.threads > 1) {
//                parallelSortImplUnsigned(task.sourceArray,
//                                         task.targetArray,
//                                         task.auxiliaryBufferOffset,
//                                         task.threads,
//                                         task.recursionDepth,
//                                         task.sourceArrawFromIndex,
//                                         task.sourceArrayToIndex);
//            } else {
//                sortImplUnsigned(task.sourceArray,
//                                 task.targetArray,
//                                 task.auxiliaryBufferOffset,
//                                 task.recursionDepth,
//                                 task.sourceArrawFromIndex,
//                                 task.sourceArrayToIndex);
//            }
        }
    }
}
