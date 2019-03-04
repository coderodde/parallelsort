package net.coderodde.util;

import net.coderodde.util.support.LongBucketSorterInputTask;
import java.util.Arrays;
import net.coderodde.util.support.SignedLongBucketInserterThread;
import net.coderodde.util.support.SignedLongBucketSizeCountingThread;
import net.coderodde.util.support.UnsignedLongBucketInserterThread;
import net.coderodde.util.support.UnsignedLongBucketSizeCountingThread;

/**
 * This class implements an efficient MSD radix sort for arrays containing 
 * {@code long} values. First, it sorts the data by the most significant byte,
 * then by second most significant byte. The actual sorting is done by scanning
 * through the data and bucketize it. Each bucket contains values that have one
 * thing in common: they have the same byte value for sorting. Then, the 
 * algorithm descends into each current bucket and sorts its content by next
 * most significant byte, and so on. 
 * <br/>
 * 
 * @author Rodion "rodde" Efremov
 * @version 1.61 (Dec 29, 2018)
 */
public final class ParallelMSDRadixsort {

    /**
     * Denotes the amount of bits for a bucket.
     */
    private static final int BITS_PER_BUCKET = 8;
    
    /**
     * The amount of buckets considered at each invocation. 
     * As <tt>log_2(256) = 8</tt>, this implies that the complexity of 
     * sequential radix sort is <tt>O(kN)</tt>, where <tt>k</tt> is 
     * between 1 and 8, inclusively.
     */
    public static final int BUCKETS = 1 << BITS_PER_BUCKET;
    
    /**
     * The mask for extracting the bucket bits.
     */
    private static final int BUCKET_MASK = 0xff;
    
    /**
     * The minimum amount of entries to sort for a thread (2^16).
     */
    private static final int THREAD_THRESHOLD = 10;
    
    /**
     * The maximum length of a range to sort using quick sort.
     */
    private static final int QUICKSORT_THRESHOLD = 2;
    
    /**
     * The index of the least significant byte. For example, the index of the
     * most significant byte is zero.
     */
    private static final int LEAST_SIGNIFICANT_BYTE_INDEX = 7;
    
    /**
     * The least length of a range to sort with quicksort. For smaller ranges
     * use insertion sort.
     */
    private static final int INSERTIONSORT_THRESHOLD = 16;
    
    // We don't need to be able to construct objects of this class.
    private ParallelMSDRadixsort() {}
    
    /**
     * Sorts sequentially the input array range {@code array[fromIndex], ...,
     * array[toIndex - 1]} into an ascending order.
     * 
     * @param array     the array holding the range to sort.
     * @param fromIndex the inclusive index of the leftmost array component.
     * @param toIndex   the exclusive index of the rightmost array component.
     */
    public static void sort(final long[] array,
                            final int fromIndex,
                            final int toIndex) {
        rangeCheck(array.length, fromIndex, toIndex);
        final int rangeLength = toIndex - fromIndex;
        
        if (rangeLength < 2) {
            // Trivially sorted.
            return;
        }
        
        final long[] buffer = Arrays.copyOfRange(array, fromIndex, toIndex);
        sortImplSigned(buffer, array, fromIndex, fromIndex, toIndex);
    }
    
    public static void sort(final long[] array) {
        sort(array, 0, array.length);
    }
    
    /***
     * Sorts a particular range of the input array.
     * 
     * @param array     the array holding the target range.
     * @param fromIndex the lowest inclusive index.
     * @param toIndex   the highest exclusive index.
     */
    public static void parallelSort(final long[] array, 
                                    final int fromIndex,
                                    final int toIndex) {
        rangeCheck(array.length, fromIndex, toIndex);
        final int rangeLength = toIndex - fromIndex;
        
        if (rangeLength < 2) {
            // Trivially sorted.
            return;
        }
        
        final long[] buffer = Arrays.copyOfRange(array, fromIndex, toIndex);
        int threads = Math.min(rangeLength / THREAD_THRESHOLD,
                               Runtime.getRuntime().availableProcessors());
        
        threads = Math.max(1, threads);
        
        if (threads > 1) {
            parallelSortImplSigned(array,
                                   buffer,
                                   fromIndex,
                                   threads,
                                   fromIndex,
                                   toIndex);
        } else {
            sortImplSigned(array,
                           buffer,
                           fromIndex,
                           fromIndex,
                           toIndex);
        }
    }    
    
    public static void parallelSort(final long[] array) {
        parallelSort(array, 0, array.length);
    }
    
    /**
     * Sorts in parallel the range {@code source[sourceArrayOffset + 
     * sourceArrayFromIndex], ...,
     * source[sourceArrayOffset + sourceArrayToIndex - 1]} placing the sorted
     * range into {@code target} at the corresponding range.
     * 
     * @param sourceArray           the source array.
     * @param targetArray           the target array.
     * @param auxiliaryBufferOffset the source array offset.
     * @param threads               the number of threads to use.
     * @param sourceArrayFromIndex  the logical starting index in the range to 
     *                              sort.
     * @param sourceArrayToIndex    the logical ending index of the range to 
     *                              sort.
     */
    private static void parallelSortImplSigned(final long[] sourceArray,
                                               final long[] targetArray,
                                               final int auxiliaryBufferOffset,
                                               final int threads,
                                               final int sourceArrayFromIndex,
                                               final int sourceArrayToIndex) {
        final int rangeLength = sourceArrayToIndex - sourceArrayFromIndex;
        
        if (rangeLength < QUICKSORT_THRESHOLD) {
            quicksort(sourceArray, 
                      sourceArrayFromIndex, 
                      sourceArrayToIndex);
            return;
        }
        
        if (threads < 2) {
            // No multithreading is suitable, sort serially.
            sortImplSigned(sourceArray, 
                           targetArray, 
                           auxiliaryBufferOffset, 
                           sourceArrayFromIndex,
                           sourceArrayToIndex);
            return;
        }

        final SignedLongBucketSizeCountingThread[] bucketSizeCounters = 
                new SignedLongBucketSizeCountingThread[threads - 1];
        
        final int subrangeLength = rangeLength / threads;
        int startIndex = sourceArrayFromIndex;
        
        for (int i = 0; i != threads - 1; i++, startIndex += subrangeLength) {
            bucketSizeCounters[i] = 
                    new SignedLongBucketSizeCountingThread(
                            sourceArray,
                            startIndex,
                            startIndex + subrangeLength);
            bucketSizeCounters[i].start();
        }
        
        SignedLongBucketSizeCountingThread lastCounterThread = 
        new SignedLongBucketSizeCountingThread(sourceArray,
                                               startIndex,
                                               sourceArrayToIndex);
        
        lastCounterThread.run();
        
        for (SignedLongBucketSizeCountingThread thread : bucketSizeCounters) {
            try {
                thread.join();
            } catch (InterruptedException ex) {
                throw new RuntimeException("Problems with multithreading.", ex);
            }
        }
        
        final int[] bucketSizeMap = new int[BUCKETS];
        final int[] startIndexMap = new int[BUCKETS];
        
        // Count the size of each bucket.
        for (int i = 0; i != bucketSizeCounters.length; i++) {
            SignedLongBucketSizeCountingThread counter = bucketSizeCounters[i];
            
            for (int j = 0; j != BUCKETS; j++) {
                bucketSizeMap[j] += counter.localBucketSizeMap[j];
            }
        }
        
        // Handle the last counter thread.
        for (int j = 0; j != BUCKETS; j++) {
            bucketSizeMap[j] += lastCounterThread.localBucketSizeMap[j];
        }
        
        // Prepare the starting indices of each bucket.
        startIndexMap[0] = sourceArrayFromIndex;
        
        for (int i = 1; i != BUCKETS; i++) {
            startIndexMap[i] = startIndexMap[i - 1] +
                               bucketSizeMap[i - 1];
        }
        
        SignedLongBucketInserterThread[] inserterThreads = 
                new SignedLongBucketInserterThread[threads - 1];
        
        int[][] processedMaps = new int[threads][BUCKETS];
        
        for (int i = 1; i != threads; i++) {
            int[] partialBucketSizeMap = 
                    bucketSizeCounters[i - 1].localBucketSizeMap; 
            
            for (int j = 0; j != BUCKETS; j++) {
                processedMaps[i][j] = 
                processedMaps[i - 1][j] + partialBucketSizeMap[j];
            }
        }
        
        startIndex = sourceArrayFromIndex;
        
        for (int i = 0; i != threads - 1; i++, startIndex += subrangeLength) {
            inserterThreads[i] = 
                    new SignedLongBucketInserterThread(
                            sourceArray,
                            targetArray,
                            startIndexMap,
                            processedMaps[i],
                            auxiliaryBufferOffset,
                            startIndex,
                            startIndex + subrangeLength);
            inserterThreads[i].start();
        }
        
        new SignedLongBucketInserterThread(sourceArray, 
                                           targetArray, 
                                           startIndexMap, 
                                           processedMaps[threads - 1], 
                                           auxiliaryBufferOffset, 
                                           startIndex, 
                                           sourceArrayToIndex).run();
        
        try {
            for (int i = 0; i != inserterThreads.length; i++) {
                inserterThreads[i].join();
            }
        } catch (InterruptedException ex) {
            throw new RuntimeException("Problems with multithreading.", ex);
        }
        
        int numberOfNonEmptyBuckets = 0;
        
        for (int i : bucketSizeMap) {
            if (i != 0) {
                numberOfNonEmptyBuckets++;
            }
        }
        
        final int spawnDegree = Math.min(numberOfNonEmptyBuckets, threads);
        IntArray[] bucketIndexListArray = new IntArray[spawnDegree];
        
        for (int i = 0; i != spawnDegree; i++) {
            bucketIndexListArray[i] = new IntArray(numberOfNonEmptyBuckets);
        }
        
        final int[] threadCountMap = new int[spawnDegree];
        
        for (int i = 0; i != spawnDegree; i++) {
            threadCountMap[i] = threads / spawnDegree;
        }
        
        for (int i = 0; i != threads % spawnDegree; i++) {
            threadCountMap[i]++;
        }
            
        IntArray nonEmptyBucketIndices = new IntArray(numberOfNonEmptyBuckets);
        
        for (int i = 0; i != BUCKETS; i++) {
            if (bucketSizeMap[i] != 0) {
                nonEmptyBucketIndices.add(i);
            }
        }
        
        quicksortBucketIndices(nonEmptyBucketIndices.array,
                               0,
                               nonEmptyBucketIndices.size,
                               bucketSizeMap);
        
        final int optimalSubrangeLength = rangeLength / spawnDegree;
        int listIndex = 0;
        int elementsPacked = 0;
        int f = 0;
        int j = 0;
        
        while (j < nonEmptyBucketIndices.size) {
            elementsPacked += bucketSizeMap[nonEmptyBucketIndices.array[j++]];
            
            if (elementsPacked >= optimalSubrangeLength
                    || j == nonEmptyBucketIndices.size) {
                elementsPacked = 0;
                
                for (int i = f; i < j; i++) {
                    bucketIndexListArray[listIndex]
                            .add(nonEmptyBucketIndices.array[i]);
                }
                
                listIndex++;
                f = j;
            }
        }
        
        LongBucketSorterInputTask[][] taskMatrix = 
                new LongBucketSorterInputTask[spawnDegree][];
        
        for (int threadIndex = 0; threadIndex != spawnDegree; threadIndex++) {
            taskMatrix[threadIndex] =
                    new LongBucketSorterInputTask
                            [bucketIndexListArray[threadIndex].size];
            
            f = 0;
            
            for (f = 0; f < bucketIndexListArray[threadIndex].size; f++) {
                IntArray bucketIndexList = bucketIndexListArray[f];
                int bucketIndex = bucketIndexList.array[f];
                
                taskMatrix[threadIndex][f++] =
                    new LongBucketSorterInputTask(
                        targetArray,
                        sourceArray,
                        threadCountMap[threadIndex],
                        1,
                        auxiliaryBufferOffset,
                        startIndexMap[bucketIndex],
                        startIndexMap[bucketIndex] + bucketSizeMap[bucketIndex],
                        startIndexMap[bucketIndex] - auxiliaryBufferOffset);
            }
        }
            
        LongSorterThread[] sorterThreads = new LongSorterThread[spawnDegree];
        
        for (int i = 0; i != spawnDegree - 1; i++) {
            sorterThreads[i] = new LongSorterThread(taskMatrix[i]);
            sorterThreads[i].start();
        }
            
        new LongSorterThread(taskMatrix[spawnDegree - 1]).run();
        
        try {
            for (int i = 0; i != spawnDegree - 1; i++) {
                sorterThreads[i].join();
            }
        } catch (InterruptedException ex) {
            throw new RuntimeException("Problems with multithreading.", ex);
        }   
    }
    
    /**
     * Sorts in parallel the range {@code source[sourceArrayOffset + fromIndex], 
     * ..., source[sourceArrayOffset + toIndex - 1]} placing the sorted range
     * into {@code target[targetArrayOffset + fromIndex], ...,
     * target[targetArrayOffset + toIndex - 1]}.
     * 
     * @param sourceArray           the source array.
     * @param targetArray           the target array.
     * @param auxiliaryBufferOffset the source array offset.
     * @param threads               the number of threads to use.
     * @param recursionDepth        the recursion depth.
     * @param sourceArrayFromIndex  the logical starting index of the range to sort.
     * @param sourceArrayToIndex    the logical ending index of the range to sort.
     */
    public static void parallelSortImplUnsigned(final long[] sourceArray,
                                                final long[] targetArray,
                                                final int auxiliaryBufferOffset,
                                                final int threads,
                                                final int recursionDepth,
                                                final int sourceArrayFromIndex,
                                                final int sourceArrayToIndex) {
        final int rangeLength = sourceArrayToIndex - sourceArrayFromIndex;
        
        if (rangeLength < QUICKSORT_THRESHOLD) {
            quicksort(sourceArray, 
                      sourceArrayFromIndex, 
                      sourceArrayToIndex);
            return;
        }
        
        if (threads < 2) {
            // No multithreading is suitable, sort serially.
            sortImplUnsigned(sourceArray, 
                           targetArray, 
                           auxiliaryBufferOffset,
                           recursionDepth,
                           sourceArrayFromIndex,
                           sourceArrayToIndex);
            return;
        }
        
        final UnsignedLongBucketSizeCountingThread[] bucketSizeCounters = 
                new UnsignedLongBucketSizeCountingThread[threads - 1];
        
        final int subrangeLength = rangeLength / threads;
        int startIndex = sourceArrayFromIndex;
        
        for (int i = 0; i != threads - 1; i++, startIndex += subrangeLength) {
            bucketSizeCounters[i] = 
                    new UnsignedLongBucketSizeCountingThread(
                            sourceArray,
                            startIndex,
                            startIndex + subrangeLength,
                            recursionDepth);
            
            bucketSizeCounters[i].start();
        }
        
        UnsignedLongBucketSizeCountingThread lastCounterThread = 
        new UnsignedLongBucketSizeCountingThread(sourceArray,
                                                 startIndex,
                                                 sourceArrayToIndex,
                                                 recursionDepth);
        
        lastCounterThread.run();
        
        for (UnsignedLongBucketSizeCountingThread thread : bucketSizeCounters) {
            try {
                thread.join();
            } catch (InterruptedException ex) {
                throw new RuntimeException("Problems with multithreading.", ex);
            }
        }
        
        final int[] bucketSizeMap = new int[BUCKETS];
        final int[] startIndexMap = new int[BUCKETS];
        
        // Count the size of each bucket.
        for (int i = 0; i != bucketSizeCounters.length; i++) {
            UnsignedLongBucketSizeCountingThread counter = bucketSizeCounters[i];
            
            for (int j = 0; j != BUCKETS; j++) {
                bucketSizeMap[j] += counter.localBucketSizeMap[j];
            }
        }
        
        // Handle the last counter thread.
        for (int j = 0; j != BUCKETS; j++) {
            bucketSizeMap[j] += lastCounterThread.localBucketSizeMap[j];
        }
        
        // Prepare the starting indices of each bucket.
        startIndexMap[0] = sourceArrayFromIndex;
        
        for (int i = 1; i != BUCKETS; i++) {
            startIndexMap[i] = startIndexMap[i - 1] +
                               bucketSizeMap[i - 1];
        }
        
        UnsignedLongBucketInserterThread[] inserterThreads = 
                new UnsignedLongBucketInserterThread[threads - 1];
        
        int[][] processedMaps = new int[threads][BUCKETS];
        
        for (int i = 1; i != threads; i++) {
            int[] partialBucketSizeMap = 
                    bucketSizeCounters[i - 1].localBucketSizeMap; 
            
            for (int j = 0; j != BUCKETS; j++) {
                processedMaps[i][j] = 
                processedMaps[i - 1][j] + partialBucketSizeMap[j];
            }
        }
        
        startIndex = sourceArrayFromIndex;
        
        for (int i = 0; i != threads - 1; i++, startIndex += subrangeLength) {
            inserterThreads[i] = 
                    new UnsignedLongBucketInserterThread(
                            sourceArray,
                            targetArray,
                            startIndexMap,
                            processedMaps[i],
                            auxiliaryBufferOffset,
                            recursionDepth,
                            startIndex,
                            startIndex + subrangeLength);
            inserterThreads[i].start();
        }
        
        new UnsignedLongBucketInserterThread(sourceArray, 
                                            targetArray, 
                                            startIndexMap, 
                                            processedMaps[threads - 1], 
                                            auxiliaryBufferOffset, 
                                            recursionDepth,
                                            startIndex, 
                                            sourceArrayToIndex).run();
        
        try {
            for (int i = 0; i != inserterThreads.length; i++) {
                inserterThreads[i].join();
            }
        } catch (InterruptedException ex) {
            throw new RuntimeException("Problems with multithreading.", ex);
        }
        
        int numberOfNonEmptyBuckets = 0;
        
        for (int i : bucketSizeMap) {
            if (i != 0) {
                numberOfNonEmptyBuckets++;
            }
        }
        
        final int spawnDegree = Math.min(numberOfNonEmptyBuckets, threads);
        IntArray[] bucketIndexListArray = new IntArray[spawnDegree];
        
        for (int i = 0; i != spawnDegree; i++) {
            bucketIndexListArray[i] = new IntArray(numberOfNonEmptyBuckets);
        }
        
        final int[] threadCountMap = new int[spawnDegree];
        
        for (int i = 0; i != spawnDegree; i++) {
            threadCountMap[i] = threads / spawnDegree;
        }
        
        for (int i = 0; i != threads % spawnDegree; i++) {
            threadCountMap[i]++;
        }
            
        IntArray nonEmptyBucketIndices = new IntArray(numberOfNonEmptyBuckets);
        
        for (int i = 0; i != BUCKETS; i++) {
            if (bucketSizeMap[i] != 0) {
                nonEmptyBucketIndices.add(i);
            }
        }
        
        quicksortBucketIndices(nonEmptyBucketIndices.array,
                               0,
                               nonEmptyBucketIndices.size,
                               bucketSizeMap);
        
        final int optimalSubrangeLength = rangeLength / spawnDegree;
        int listIndex = 0;
        int elementsPacked = 0;
        int f = 0;
        int j = 0;
        
        while (j < nonEmptyBucketIndices.size) {
            elementsPacked += bucketSizeMap[nonEmptyBucketIndices.array[j++]];
            
            if (elementsPacked >= optimalSubrangeLength
                    || j == nonEmptyBucketIndices.size) {
                elementsPacked = 0;
                
                for (int i = f; i < j; i++) {
                    bucketIndexListArray[listIndex]
                            .add(nonEmptyBucketIndices.array[i]);
                }
                
                listIndex++;
                f = j;
            }
        }
        
        LongBucketSorterInputTask[][] taskMatrix = 
                new LongBucketSorterInputTask[spawnDegree][];
        
        for (int threadIndex = 0; threadIndex != spawnDegree; threadIndex++) {
            taskMatrix[threadIndex] =
                    new LongBucketSorterInputTask[bucketIndexListArray[threadIndex].size];
            
            for (int i = 0; i != bucketIndexListArray[threadIndex].size; i++) {
                taskMatrix[threadIndex][i] =
                        new LongBucketSorterInputTask(
                                targetArray,
                                sourceArray,
                                threadCountMap[threadIndex],
                                1,
                                auxiliaryBufferOffset,
                                sourceArrayFromIndex - auxiliaryBufferOffset,
                                sourceArrayToIndex   - auxiliaryBufferOffset,
                                sourceArrayFromIndex);
            }
        }
            
        LongSorterThread[] sorterThreads = new LongSorterThread[spawnDegree];
        
        for (int i = 0; i != spawnDegree - 1; i++) {
            sorterThreads[i] = new LongSorterThread(taskMatrix[i]);
            sorterThreads[i].start();
        }
            
        new LongSorterThread(taskMatrix[spawnDegree - 1]).run();
        
        try {
            for (int i = 0; i != spawnDegree - 1; i++) {
                sorterThreads[i].join();
            }
        } catch (InterruptedException ex) {
            throw new RuntimeException("Problems with multithreading.", ex);
        }   
    }
    
    /**
     * Performs serial sorting over a requested range. The values of 
     * {@code fromIndex} and {@code toIndex} must be set accordingly by the
     * caller method.
     * 
     * @param sourceArray           the array that contains all the correct values
     *                              but in an arbitrary order.
     * @param targetArray           the array holding the targeted range to sort.
     * @param auxiliaryBufferOffset the offset of the auxiliary buffer.
     * @param recursionDepth        the depth of recursion. The value of zero stands
     *                              for the most-significant byte.
     * @param sourceArrayFromIndex  the starting, inclusive index into the actual 
     *                              range in the source array.
     * @param sourceArrayToIndex    the ending, exclusive index into the actual
     *                              range in the source array.
     */
    static final void sortImplSigned(final long[] sourceArray,
                                     final long[] targetArray,
                                     final int auxiliaryBufferOffset,
                                     final int sourceArrayFromIndex,
                                     final int sourceArrayToIndex) {
        final int rangeLength = sourceArrayToIndex - sourceArrayFromIndex;
        
        if (rangeLength <= QUICKSORT_THRESHOLD) {
            quicksort(sourceArray,
                      sourceArrayFromIndex,
                      sourceArrayToIndex);
            return;
        }
        
        final int[] bucketSizeMap = new int[BUCKETS];
        final int[] startIndexMap = new int[BUCKETS];
        final int[] processedMap  = new int[BUCKETS];
        
        // Find out the size of each bucket in the current source array range.
        for (int i = sourceArrayFromIndex; 
                 i != sourceArrayToIndex;
                 i++) {
            final int bucketIndex = getSignedBucketIndex(sourceArray[i]);
            bucketSizeMap[bucketIndex]++;
        }
        
        // Compute the indices of the first element in each bucket.
        startIndexMap[0] = sourceArrayFromIndex /*- (recursionDepth == 0 ?
                                                   0 :
                                                   auxiliaryBufferOffset)*/;
        
        for (int i = 1; i != BUCKETS; i++) {
            startIndexMap[i] = startIndexMap[i - 1] +
                               bucketSizeMap[i - 1];
        }
        
        for (int i = sourceArrayFromIndex; 
                 i != sourceArrayToIndex;
                 i++) {
            final int bucketIndex = getSignedBucketIndex(sourceArray[i]);
            final int targetElementIndex = startIndexMap[bucketIndex] +
                                           processedMap [bucketIndex] -
                                           auxiliaryBufferOffset;
            processedMap[bucketIndex]++;
            targetArray[targetElementIndex] = sourceArray[i]; 
        } 
        
        for (int i = 0; i != BUCKETS; i++) {
            if (bucketSizeMap[i] != 0) {
                // We translate each index 'i' in 'sourceArray' to 
                // an index 'j' in 'targetArray'. In other words,
                // 'j = i - auxiliaryBufferOffset'.
                sortImplUnsigned(targetArray,
                                 sourceArray,
                                 auxiliaryBufferOffset,
                                 1,
                                 startIndexMap[i] - auxiliaryBufferOffset,
                                 startIndexMap[i] - auxiliaryBufferOffset 
                                                  + bucketSizeMap[i]); 
            }
        }
    }
    
    /**
     * Performs serial sorting over a requested range. The values of 
     * {@code fromIndex} and {@code toIndex} must be set accordingly by the
     * caller method.
     * 
     * @param sourceArray           the array that contains all the correct 
     *                              values but in an arbitrary order.
     * @param targetArray           the array holding the targeted range to 
     *                              sort.
     * @param auxiliaryBufferOffset the offset of the auxiliary buffer.
     * @param recursionDepth        the depth of recursion. The value of zero 
     *                              stands for the most-significant byte.
     * @param sourceArrayFromIndex  the starting, inclusive index into the 
     *                              actual range in the source array.
     * @param sourceArrayToIndex    the ending, exclusive index into the actual
     *                              range in the source array.
     */
    static final void sortImplUnsigned(final long[] sourceArray,
                                       final long[] targetArray,
                                       final int auxiliaryBufferOffset,
                                       final int recursionDepth,
                                       final int sourceArrayFromIndex,
                                       final int sourceArrayToIndex) {
        final int rangeLength = sourceArrayToIndex - sourceArrayFromIndex;
        
        if (rangeLength <= /* QUICKSORT_THRESHOLD */ 2) {
            if (recursionDepth % 2 == 0) {
                // 'sourceArray' is the actual input array.
                quicksort(sourceArray,
                          sourceArrayFromIndex,
                          sourceArrayToIndex);
            } else {
                // 'sourceArray' is actually the auxiliary buffer, copy to 
                // corresponding range in 'targetArray'.
                quicksort(sourceArray,
                          sourceArrayFromIndex,
                          sourceArrayToIndex);
                
                System.arraycopy(sourceArray, 
                                 sourceArrayFromIndex,
                                 targetArray,
                                 sourceArrayFromIndex + auxiliaryBufferOffset,
                                 rangeLength);
            }
            
            return;
        }
        
        final int[] bucketSizeMap = new int[BUCKETS];
        final int[] startIndexMap = new int[BUCKETS];
        final int[] processedMap  = new int[BUCKETS];
        
        // Find out the size of each bucket in the current source array range.
        for (int i = sourceArrayFromIndex; 
                 i != sourceArrayToIndex;
                 i++) {
            final int bucketIndex = getUnsignedBucketIndex(sourceArray[i], 
                                                           recursionDepth);
            bucketSizeMap[bucketIndex]++;
        }
        
        // Compute the indices of the first element in each bucket.
        startIndexMap[0] = sourceArrayFromIndex /*- (recursionDepth == 0 ?
                                                   0 :
                                                   auxiliaryBufferOffset)*/;
        for (int i = 1; i != BUCKETS; i++) {
            startIndexMap[i] = startIndexMap[i - 1] +
                               bucketSizeMap[i - 1];
        }
        
        // Now we know where each bucket begins in the opposite array.
        // Insert the actual, unsorted buckets into their correct buckets in the
        // target array.
        if (recursionDepth % 2 == 0) {
            // Here, 'sourceArray' is the actual input array and 'targetArray'
            // is the actuall auxiliary buffer.
            for (int i = sourceArrayFromIndex; 
                     i != sourceArrayToIndex;
                     i++) {
                final int bucketIndex = getUnsignedBucketIndex(sourceArray[i],
                                                               recursionDepth);
                final int elementIndex = startIndexMap[bucketIndex] +
                                          processedMap[bucketIndex] -
                                          auxiliaryBufferOffset;
                processedMap[bucketIndex]++;
                targetArray[elementIndex] = sourceArray[i]; 
            } 
            
            for (int i = 0; i != BUCKETS; i++) {
                if (bucketSizeMap[i] != 0) {
                    // We translate each index 'i' in 'sourceArray' to 
                    // an index 'j' in 'targetArray'. In other words,
                    // 'j = i - auxiliaryBufferOffset'.
                    sortImplUnsigned(targetArray,
                                     sourceArray,
                                     auxiliaryBufferOffset,
                                     recursionDepth + 1,
                                     startIndexMap[i] - auxiliaryBufferOffset,
                                     startIndexMap[i] - auxiliaryBufferOffset 
                                                      + bucketSizeMap[i]); 
                }
            }
        } else {
            for (int i = sourceArrayFromIndex; 
                     i != sourceArrayToIndex;
                     i++) {
                final int bucketIndex = getUnsignedBucketIndex(sourceArray[i],
                                                               recursionDepth);
                final int elementIndex = startIndexMap[bucketIndex] +
                                          processedMap[bucketIndex] +
                                          auxiliaryBufferOffset;
                processedMap[bucketIndex]++;
                targetArray[elementIndex] = sourceArray[i];
            }
            
            for (int i = 0; i != BUCKETS; i++) {
                if (bucketSizeMap[i] != 0) {
                    // We translate each index 'i' in 'sourceArray' to 
                    // an index 'j' in 'targetArray'. In other words,
                    // 'j = i + auxiliaryBufferOffset'. Unlike above, note the
                    // minus signs.
                    sortImplUnsigned(targetArray,
                                     sourceArray,
                                     auxiliaryBufferOffset,
                                     recursionDepth + 1,
                                     startIndexMap[i] + auxiliaryBufferOffset,
                                     startIndexMap[i] + auxiliaryBufferOffset 
                                                      + bucketSizeMap[i]); 
                }
            }
        }
    }
    
    private static final class LongBucketInserterThread extends Thread {
        
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
         * The offset in the source array.
         */
        private final int sourceArrayOffset;
        
        /**
         * The offset in the target array.
         */
        private final int targetArrayOffset;
        
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
         * Stores the recursion depth. 7 is for the most significant byte of a 
         * {@code long}, 0 for the least significant.
         */
        private final int recursionDepth;
        
        /**
         * The smallest inclusive index of the range in the source array. 
         */
        private final int fromIndex;
        
        /**
         * The last exclusive index of the range in the source array.
         */
        private final int toIndex;
        
        /**
         * Constructs the thread object for inserting the elements to their 
         * corresponding buckets in the opposite array.
         * 
         * @param startIndexMap  the map of bucket starting indices.
         * @param processedMap   the map counting number of inserted elements.
         * @param sourceArray    the source array.
         * @param targetArray    the target array.
         * @param recursionDepth the current depth of recursion.
         * @param fromIndex      the smallest, inclusive index of the range to 
         *                       bucketize.
         * @param toIndex        the largest, exclusive index of the range to 
         *                       bucketize.
         * @param offset         the leading offset.
         */
        LongBucketInserterThread(final long[] sourceArray,
                                 final long[] targetArray,
                                 final int sourceArrayOffset,
                                 final int targetArrayOffset,
                                 final int[] startIndexMap,
                                 final int[] processedMap,
                                 final int recursionDepth,
                                 final int fromIndex,
                                 final int toIndex) {
            this.startIndexMap     = startIndexMap;
            this.processedMap      = processedMap;
            this.sourceArrayOffset = sourceArrayOffset;
            this.targetArrayOffset = targetArrayOffset;
            this.sourceArray       = sourceArray;
            this.targetArray       = targetArray;
            this.recursionDepth    = recursionDepth;
            this.fromIndex         = fromIndex;
            this.toIndex           = toIndex;
        }
        
        @Override
        public void run() {
            for (int i = fromIndex + sourceArrayOffset; 
                    i != toIndex + sourceArrayOffset; i++) {
                int bucketIndex = getUnsignedBucketIndex(sourceArray[i], 
                                                         recursionDepth);
                targetArray[startIndexMap[bucketIndex] + 
                            processedMap [bucketIndex] + targetArrayOffset] =
                        sourceArray[i];
            }
        }
    }
    
    private static final class LongSorterThread extends Thread {
    
        private final LongBucketSorterInputTask[] taskList;
        
        LongSorterThread(LongBucketSorterInputTask[] taskList) {
            this.taskList = taskList;
        }
        
        @Override
        public void run() {
//            for (LongTask longTask : taskList) {
//                if (longTask.threads > 1) {
//                    parallelSortImpl(longTask.sourceArray,
//                                     longTask.targetArray,
//                                     longTask.sourceArrayOffset,
//                                     longTask.threads,
//                                     longTask.recursionDepth,
//                                     longTask.fromIndex,
//                                     longTask.toIndex);
//                } else {
//                    sortImplUnsigned(longTask.sourceArray,
//                             longTask.targetArray,
//                             longTask.sourceArrayOffset,
//                             longTask.recursionDepth,
//                             longTask.fromIndex,
//                             longTask.toIndex);
//                }
//            }
        }
    }
    
    private static final class LongTask {
        
        final long[] sourceArray;
        final long[] targetArray;
        final int sourceArrayOffset;
        final int threads;
        final int recursionDepth;
        final int fromIndex;
        final int toIndex;
        
        LongTask(final long[] sourceArray,
                 final long[] targetArray,
                 final int sourceArrayOffset,
                 final int threads,
                 final int recursionDepth,
                 final int fromIndex,
                 final int toIndex) {
            this.sourceArray       = sourceArray;
            this.targetArray       = targetArray;
            this.sourceArrayOffset = sourceArrayOffset;
            this.threads           = threads;
            this.recursionDepth    = recursionDepth;
            this.fromIndex         = fromIndex;
            this.toIndex           = toIndex;
        }
    }
        
    /**
     * This class implements a simple array-based list.
     */
    private static final class IntArray {
        
        /**
         * The actual array storing the integer elements.
         */
        final int[] array;
        
        /**
         * The number of elements stored in this integer array.
         */
        private int size;
        
        IntArray(int arrayLength) {
            this.array = new int[arrayLength];
            this.size = 0;
        }
        
        void add(int i) {
            array[size++] = i;
        }
        
        int size() {
            return size;
        }
    }
    
    static final int getSignedBucketIndex(final long key) {
        final int bitShift = (Long.BYTES - 1) * Byte.SIZE;
        return (int)((key >>> bitShift) ^ 0b1000_0000);
        // ... & 0b1000_0000 flips the sign bit so that all the negative value
        // buckets end up before the positive ones.
//        return (int)((key >>> bitShift) & 0b1000_0000L);
    }
    
    static final int getUnsignedBucketIndex(final long key,
                                            final int byteIndex) {
        final int bitShift = (Long.BYTES - byteIndex - 1) * Byte.SIZE;
        return (int)(key >>> bitShift) & BUCKET_MASK;
    }

    public static void quicksort(long[] array, 
                                 int fromIndex, 
                                 int toIndex) {
        while (true) {
            int rangeLength = toIndex - fromIndex;

            if (rangeLength < 2) {
                return;
            }

            if (rangeLength < INSERTIONSORT_THRESHOLD) {
                insertionsort(array, fromIndex, toIndex);
                return;
            }
            
            int distance = rangeLength / 4;
            long a = array[fromIndex + distance];
            long b = array[fromIndex + (rangeLength >>> 1)];
            long c = array[toIndex - distance];
            long pivot = medianLong(a, b, c);
            int leftPartitionLength = 0;
            int rightPartitionLength = 0;
            int index = fromIndex;

            while (index < toIndex - rightPartitionLength) {
                long current = array[index];

                if (current > pivot) {
                    ++rightPartitionLength;
                    swap(array, toIndex - rightPartitionLength, index);
                } else if (current < pivot) {
                    swap(array, fromIndex + leftPartitionLength, index);
                    ++index;
                    ++leftPartitionLength;
                } else {
                    ++index;
                }
            }

            if (leftPartitionLength < rightPartitionLength) {
                quicksort(array, fromIndex, fromIndex + leftPartitionLength);
                fromIndex = toIndex - rightPartitionLength;
            } else {
                quicksort(array, toIndex - rightPartitionLength, toIndex);
                toIndex = fromIndex + leftPartitionLength;
            }
        }
    }

    private static final void swap(int[] array, int index1, int index2) {
        int tmp = array[index1];
        array[index1] = array[index2];
        array[index2] = tmp;
    }
    
    private static final void swap(long[] array, int index1, int index2) {
        long tmp = array[index1];
        array[index1] = array[index2];
        array[index2] = tmp;
    }
        
    public static void insertionsort(long[] array, int fromIndex, int toIndex) {
        for (int i = fromIndex + 1; i < toIndex; ++i) {
            long current = array[i];
            int j = i  - 1;

            while (j >= fromIndex && array[j] > current) {
                array[j + 1] = array[j];
                --j;
            }

            array[j + 1] = current;
        }
    }
    
    public static void quicksortBucketIndices(final int[] indexArray,
                                       int fromIndex,
                                       int toIndex,
                                       final int[] bucketSizeMap) {
        while (true) {
            final int rangeLength = toIndex - fromIndex;
            
            if (rangeLength < 2) {
                return;
            }
            
            if (rangeLength < INSERTIONSORT_THRESHOLD) {
                insertionsortBucketIndices(indexArray,
                                           fromIndex,
                                           toIndex,
                                           bucketSizeMap);
                return;
            }
            
            final int distance = rangeLength / 4;
            final int bucketSizeA = 
                    bucketSizeMap[indexArray[fromIndex + distance]]; 
            
            final int bucketSizeB = 
                    bucketSizeMap[indexArray[fromIndex + distance * 2]];
            
            final int bucketSizeC = 
                    bucketSizeMap[indexArray[toIndex - distance]];
            
            final int bucketSizePivot = medianInt(bucketSizeA,
                                                  bucketSizeB,
                                                  bucketSizeC);
            int leftPartitionLength  = 0;
            int rightPartitionLength = 0;
            int index = fromIndex;
            
            while (index < toIndex - rightPartitionLength) {
                final int currentElementValue = indexArray[index];
                final int currentElementKey = 
                        bucketSizeMap[currentElementValue];
                
                if (currentElementKey > bucketSizePivot) {
                    rightPartitionLength++;
                    swap(indexArray,
                         toIndex - rightPartitionLength,
                         index);
                } else if (currentElementKey < bucketSizePivot) {
                    swap(indexArray,
                         fromIndex + leftPartitionLength,
                         index);
                    index++;
                    leftPartitionLength++;
                } else {
                    index++;
                }
            }
            
            if (leftPartitionLength < rightPartitionLength) {
                quicksortBucketIndices(indexArray, 
                                       fromIndex,
                                       fromIndex + leftPartitionLength, 
                                       bucketSizeMap);
                fromIndex = toIndex - rightPartitionLength;
            } else {
                quicksortBucketIndices(indexArray,
                                       toIndex - rightPartitionLength, 
                                       toIndex,
                                       bucketSizeMap);
                toIndex = fromIndex + leftPartitionLength;
            }
        }
    }
    
    private static int medianInt(final int a, 
                                 final int b, 
                                 final int c) {
        if (a <= b) {
            if (c <= a) {
                return a;
            }

            return b <= c ? b : c;
        } 

        if (c <= b) {
            return b;
        }

        return a <= c ? a : c;
    } 
    
    private static long medianLong(final long a, 
                                   final long b, 
                                   final long c) {
        if (a <= b) {
            if (c <= a) {
                return a;
            }

            return b <= c ? b : c;
        } 

        if (c <= b) {
            return b;
        }

        return a <= c ? a : c;
    } 
    
    /**
     * Sorts the array of bucket indices using size as the sorting key. Note
     * that this method will crash if {@code array} contains negative values.
     * The convention is that {@code bucketSizeMap[i]} holds the size of the 
     * bucket number {@code i}.
     * 
     * @param indexArray    the array of bucket indices.
     * @param fromIndex     the starting, inclusive index of the range to sort.
     * @param toIndex       the ending, exclusive index of the range to sort.
     * @param bucketSizeMap the array holding the bucket sizes.
     */
    static void insertionsortBucketIndices(final int[] indexArray,
                                           final int fromIndex,
                                           final int toIndex,
                                           final int[] bucketSizeMap) {
        for (int i = fromIndex + 1; i < toIndex; i++) {
            final int currentValue = indexArray[i];
            final int currentKey = bucketSizeMap[currentValue];
            int j = i - 1;
            
            while (j >= fromIndex 
                    && bucketSizeMap[indexArray[j]] > currentKey) {
                indexArray[j + 1] = indexArray[j];
                j--;
            }
            
            indexArray[j + 1] = currentValue;
        }
    }
    
    /**
     * Checks that {@code fromIndex} and {@code toIndex} are in the range and 
     * throws an exception if they aren't.
     */
    private static void rangeCheck(int arrayLength, int fromIndex, int toIndex) {
        if (fromIndex > toIndex) {
            throw new IllegalArgumentException(
                    "fromIndex(" + fromIndex + ") > toIndex(" + toIndex + ")");
        }
        if (fromIndex < 0) {
            throw new ArrayIndexOutOfBoundsException(fromIndex);
        }
        if (toIndex > arrayLength) {
            throw new ArrayIndexOutOfBoundsException(toIndex);
        }
    }
}