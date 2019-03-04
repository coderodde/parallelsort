package net.coderodde.util.support;

/**
 * This class implements a thread for counting the bucket sizes of an subarray.
 * It takes into account the fact that the values may be negative. 
 * 
 * @author Rodion "rodde" Efremov
 * @version 1.6 (Jan 12, 2019)
 */
public final class SignedLongBucketSizeCountingThread extends Thread {
    
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
    public final int[] localBucketSizeMap;

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
//    private final int recursionDepth;
    
    public SignedLongBucketSizeCountingThread(final long[] array,
                                              final int fromIndex,
                                              final int toIndex) {
        this.localBucketSizeMap = new int[BUCKETS];
        this.array              = array;
        this.fromIndex          = fromIndex;
        this.toIndex            = toIndex;
    }
    
    @Override
    public void run() {
        for (int i = fromIndex; i != toIndex; i++) {
            localBucketSizeMap[getSignedBucketIndex(array[i])]++;
        }
    }
    
    /**
     * Returns the bucket index for the {@code key} long value. We do nothing 
     * more but shift to the right 7 bytes positions and flip the most
     * significant bit in the remaining byte. This way we make sure that all
     * negative keys will preceed the positive keys in the sorted array.
     * 
     * @param key the key whose bucket index to compute.
     * @return the index of the bucket the input key belongs to.
     */
    static final int getSignedBucketIndex(final long key) {
        final int bitShift = (Long.BYTES - 1) * Byte.SIZE;
        return (int)((key >>> bitShift) ^ 0b1000_0000);
        // ... & 0b1000_0000 flips the sign bit so that all the negative value
        // buckets end up before the positive ones.
    }
}
