package net.coderodde.util;

/**
 * This class implements a thread 
 * @author Rodion "rodde" Efremov
 * @version 1.6 (Jan 12, 2019)
 */
final class UnsignedLongBuckeSizeCountingThread extends Thread {
    
    /**
     * The number of distinct buckets considered at each sort invocation. This
     * effectively implies that the sort considers each long value one byte a 
     * time.
     */
    private static final int BUCKETS = 256;
    
    /**
     * Used for extracting a least-significant byte of a {@code long} value.
     */
    private static final int BUCKET_MASK = 0xff;
    
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
    
    /**
     * The ordinal of the byte being considered by this thread. The most-
     * significant byte has an ordinal of zero.
     */
    private final int recursionDepth;
    
    LongBuckeSizetCountingThreadUnsigned(final long[] array,
                                         final int fromIndex,
                                         final int toIndex,
                                         final int recursionDepth) {
        this.localBucketSizeMap = new int[BUCKETS];
        this.array              = array;
        this.fromIndex          = fromIndex;
        this.toIndex            = toIndex;
        this.recursionDepth     = recursionDepth;
    }
    
    @Override
    public void run() {
        for (int i = fromIndex; i != toIndex; i++) {
            localBucketSizeMap[getUnsignedBucketIndex(array[i],
                                                      recursionDepth)]++;
        }
    }
    
    static final int getUnsignedBucketIndex(final long key,
                                            final int byteIndex) {
        final int bitShift = (Long.BYTES - byteIndex - 1) * Byte.SIZE;
        return (int)(key >>> bitShift) & BUCKET_MASK;
    }
}
