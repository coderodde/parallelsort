package net.coderodde.util;

/**
 * This class implements the thread type that inserts elements in the source 
 * array to its correct bucket in the target
 * @author rodde
 */
final class UnsignedLongBucketInserterThread extends Thread {
    
    /*'
     * The number of bits to shift to the right in order to extract the least-
     * significant byte which is considered as the bucket index.
     */
    private final int bitShift = (Long.BYTES - 1) * Byte.SIZE;

    /**
     * The array from which the elements are being moved to the target 
     * array.
     */
    private final long[] sourceArray;

    /**
     * The array to which the source array elements are being moved.
     */
    private final long[] targetArray;

    /**
     * Maps the bucket number {@code i} to the index at which it occurs in 
     * the array being processed.
     */
    private final int[] startIndexMap;

    /**
     * Keeps track of how many elements belonging to a particular bucket 
     * were inserted into their new bucket in the opposite array.
     */
    private final int[] processedMap;

    /**
     * Holds the offset with respect to the auxiliary buffer.
     */
    private final int auxiliaryBufferOffset;
    
    /**
     * Specifies the index of the target byte. Numeration starts from the most-
     * significant byte.
     */
    private final int recursionDepth;
    
    /**
     * The smallest inclusive index of the range in the source array. 
     */
    private final int sourceArrayFromIndex;

    /**
     * The last exclusive index of the range in the source array.
     */
    private final int sourceArrayToIndex;

    /**
     * Constructs the thread object for inserting the elements to their 
     * corresponding buckets in the opposite array.
     * 
     * @param sourceArray           the source array.
     * @param targetArray           the target array.
     * @param startIndexMap         the map of bucket starting indices.
     * @param processedMap          the map counting number of inserted elements.
     * @param auxiliaryBufferOffset the smallest, inclusive index of the range to 
     *                              bucketize.
     * @param sourceArrayFromIndex  the largest, exclusive index of the range to 
     *                              bucketize.
     * @param sourceArrayToIndex    the leading offset.
     */
    UnsignedLongBucketInserterThread(final long[] sourceArray,
                                     final long[] targetArray,
                                     final int[] startIndexMap,
                                     final int[] processedMap,
                                     final int auxiliaryBufferOffset,
                                     final int recursionDepth,
                                     final int sourceArrayFromIndex,
                                     final int sourceArrayToIndex) {
        this.sourceArray           = sourceArray;
        this.targetArray           = targetArray;
        this.startIndexMap         = startIndexMap;
        this.processedMap          = processedMap;
        this.auxiliaryBufferOffset = auxiliaryBufferOffset;
        this.recursionDepth        = recursionDepth;
        this.sourceArrayFromIndex  = sourceArrayFromIndex;
        this.sourceArrayToIndex    = sourceArrayToIndex;
    }

    @Override
    public void run() {
        if (recursionDepth % 2 == 1) {
            for (int i = sourceArrayFromIndex; 
                     i != sourceArrayToIndex; 
                     i++) {
                final long currentSourceElement = sourceArray[i];
                final int bucketIndex = 
                        getSignedBucketIndex(currentSourceElement);
                final int targetElementIndex = startIndexMap[bucketIndex] + 
                                               processedMap [bucketIndex] +
                                               auxiliaryBufferOffset;
                processedMap[bucketIndex]++;
                targetArray[targetElementIndex] = currentSourceElement;
            }
        } else {
            for (int i = sourceArrayFromIndex; 
                     i != sourceArrayToIndex; 
                     i++) {
                final long currentSourceElement = sourceArray[i];
                final int bucketIndex = 
                        getSignedBucketIndex(currentSourceElement);
                // Note the minus sign in the next statement.
                final int targetElementIndex = startIndexMap[bucketIndex] + 
                                               processedMap [bucketIndex] -
                                               auxiliaryBufferOffset;
                processedMap[bucketIndex]++;
                targetArray[targetElementIndex] = currentSourceElement;
            }
        }
    }
    
    private static final int getSignedBucketIndex(final long key) {
        final int bitShift = (Long.BYTES - 1) * Byte.SIZE;
        return (int)((key >>> bitShift) ^ 0b1000_0000);
    }
}