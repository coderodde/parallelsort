package net.coderodde.util;

/**
 * This class implements a thread 
 * @author Rodion "rodde" Efremov
 * @version 1.6 (Jan 12, 2019)
 */
final class SignedLongBucketSizeCountingThread extends Thread {
    
    /**
     * The number of distinct buckets considered at each sort invocation. This
     * effectively implies that the sort considers each long value one byte a 
     * time.
     */
    private static final int BUCKETS = 256;
    
    /**
     * The bucket size map. After this thread finishes, 
     * {@code localBucketSizeMap[i]} will contain the number of elements in
     * bucket {@code i}.
     */
    final int[] localBucketSizeMap;

    /**
     * The array holding the range to process.
     */
    private final long[] array;
    
    /**
     * The index of the first element in the range.
     */
    private final int fromIndex;

    /**
     * The least index past the last element in the range.
     */
    private final int toIndex;
    
    SignedLongBucketSizeCountingThread(final long[] array,
                                       final int fromIndex,
                                       final int toIndex) {
        this.localBucketSizeMap = new int[BUCKETS];
        this.array     = array;
        this.fromIndex = fromIndex;
        this.toIndex   = toIndex;
    }
    
    @Override
    public void run() {
        for (int i = fromIndex; i != toIndex; i++) {
            localBucketSizeMap[getSignedBucketIndex(array[i])]++;
        }
    }
    
    private static final int getSignedBucketIndex(final long key) {
        final int bitShift = (Long.BYTES - 1) * Byte.SIZE;
        return (int)((key >>> bitShift) ^ 0b1000_0000);
    }
}
