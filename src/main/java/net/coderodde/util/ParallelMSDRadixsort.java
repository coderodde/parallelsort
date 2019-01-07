package net.coderodde.util;

import java.util.Arrays;

/**
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
    private static final int BUCKETS = 1 << BITS_PER_BUCKET;
    
    /**
     * The mask for extracting the bucket bits.
     */
    private static final int BUCKET_MASK = BUCKETS - 1;
    
    /**
     * The mask needed to manipulate the sign bit.
     */
    private static final long SIGN_MASK = 1L << 63;
    
    /**
     * The minimum amount of entries to sort for a thread (2^16).
     */
    private static final int THREAD_THRESHOLD = 65536;
    
    /**
     * The maximum length of a range to sort using quick sort.
     */
    private static final int QUICKSORT_THRESHOLD = 4096;
    
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
    
    // This class is a singleton.
    private ParallelMSDRadixsort() {}
    
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
        final int rangeLength = toIndex - fromIndex;
        
        if (rangeLength < 2) {
            // Trivially sorted.
            return;
        }
        
        final long[] buffer = Arrays.copyOfRange(array, fromIndex, toIndex);
        int threads = Math.min(rangeLength / THREAD_THRESHOLD,
                           Runtime.getRuntime().availableProcessors());
        
        threads = Math.max(1, threads);
        
        parallelSortImpl(array, 
                         buffer,
                         fromIndex,
                         threads, 
                         0, 
                         fromIndex, 
                         toIndex);
    }    
    
    public static void parallelSort(final long[] array) {
        parallelSort(array, 0, array.length);
    }
    
    
    /**
     * Sorts in parallel the range {@code source[sourceArrayOffset + fromIndex], 
     * ..., source[sourceArrayOffset + toIndex - 1]} placing the sorted range
     * into {@code target[targetArrayOffset + fromIndex], ...,
     * target[targetArrayOffset + toIndex - 1]}.
     * 
     * @param sourceArray       the source array.
     * @param targetArray       the target array.
     * @param sourceArrayOffset the source array offset.
     * @param targetArrayOffset the target array offset
     * @param threads           the number of threads to use.
     * @param recursionDepth    the recursion depth.
     * @param fromIndex         the logical starting index of the range to sort.
     * @param toIndex           the logical ending index of the range to sort.
     * @param offset 
     */
    private static void parallelSortImpl(final long[] sourceArray,
                                         final long[] targetArray,
                                         final int sourceArrayOffset,
                                         final int threads,
                                         final int recursionDepth, 
                                         final int fromIndex,
                                         final int toIndex) {
        sortImpl(sourceArray,
                 targetArray,
                 sourceArrayOffset,
                 0,
                 fromIndex,
                 toIndex);
//        final int rangeLength = toIndex - fromIndex;
//        
//        if (rangeLength < QUICKSORT_THRESHOLD) {
//            // The most significant byte of a long value has recursion depth of
//            // 0, and the least significant byte of a long value has recursion
//            // depth of 7. When the recursionDepth is even (0, 2, 4, 6), the 
//            // data to sort is the original array being passed to the entire 
//            // sort.
//            if (recursionDepth % 2 == 0) {
//                // Once here, just sort in-plaace the range.
//                quicksort(sourceArray, 
//                          fromIndex, 
//                          toIndex);
//            } else {
//                // The subrange to sort is in the auxiliary buffer.
//                quicksort(targetArray,
//                          targetArrayOffset + fromIndex, 
//                          targetArrayOffset + toIndex);
//                
//                // Now just copy the sorted subrange to the source array.
//                System.arraycopy(targetArray,
//                                 targetArrayOffset + fromIndex, 
//                                 sourceArray, 
//                                 fromIndex,
//                                 rangeLength);
//            }
//            
//            return;
//        }
//        
//        if (threads < 2) {
//            // No multithreading is suitable, sort serially.
//            sortImpl(sourceArray, 
//                     targetArray,
//                     sourceArrayOffset,
//                     targetArrayOffset,
//                     recursionDepth,
//                     fromIndex,
//                     toIndex);
//            return;
//        }
//        
//        final LongBucketCountingThread[] counters = 
//                new LongBucketCountingThread[threads];
//        
//        
//        final int subrangeLength = rangeLength / threads;
//        int startIndex = fromIndex + sourceArrayOffset;
//        
//        for (int i = 0; i != threads - 1; i++, startIndex += subrangeLength) {
//            counters[i] = 
//                    new LongBucketCountingThread(sourceArray,
//                                                 recursionDepth,
//                                                 startIndex,
//                                                 startIndex + subrangeLength);
//        }
//        
//        new LongBucketCountingThread(sourceArray,
//                                     recursionDepth,
//                                     startIndex,
//                                     sourceArrayOffset + toIndex).run();
//        
//        try {
//            for (int i = 0; i != threads - 1; i++) {
//                counters[i].join();
//            }
//        } catch (InterruptedException ex) {
//            throw new IllegalStateException(
//                    "Something happened with multithreading. Message: " +
//                            ex.getMessage());
//        }
//        
//        final int[] bucketSizeMap = new int[BUCKETS];
//        final int[] startIndexMap = new int[BUCKETS];
//        
//        // Count the size of each bucket.
//        for (int i = 0; i != threads; i++) {
//            LongBucketCountingThread counter = counters[i];
//            
//            for (int j = 0; j != BUCKETS; j++) {
//                bucketSizeMap[j] += counter.localBucketSizeMap[j];
//            }
//        }
//        
//        // Prepare the starting indices of each bucket.
//        startIndexMap[0] = fromIndex + sourceArrayOffset;
//        
//        // Compute where each bucket should start.
//        for (int i = 1; i != BUCKETS; i++) {
//            startIndexMap[i] = startIndexMap[i - 1] + 
//                               bucketSizeMap[i - 1];
//        }
//        
//        LongBucketInserterThread[] inserters = 
//                new LongBucketInserterThread[threads - 1];
//        
//        int[][] processedMaps = new int[threads][BUCKETS];
//        
//        for (int i = 1; i != threads; i++) {
//            int[] partialBucketSizeMap = counters[i - 1].localBucketSizeMap;
//            
//            for (int j = 0; j != BUCKETS; j++) {
//                processedMaps[i][j] = 
//                        processedMaps[i - 1][j] + partialBucketSizeMap[j];
//            }
//        }
//        
//        startIndex = fromIndex + sourceArrayOffset;
//        
//        for (int i = 0; i != threads - 1; i++, startIndex += subrangeLength) {
//            inserters[i] =
//                    new LongBucketInserterThread(sourceArray,
//                                                 targetArray, 
//                                                 sourceArrayOffset, 
//                                                 targetArrayOffset,
//                                                 startIndexMap,
//                                                 bucketSizeMap, 
//                                                 recursionDepth, 
//                                                 fromIndex,     
//                                                 toIndex);
//            inserters[i].start();
//        }
//        
//        new LongBucketInserterThread(sourceArray,
//                                     targetArray,
//                                     sourceArrayOffset,
//                                     targetArrayOffset,
//                                     startIndexMap,
//                                     bucketSizeMap,
//                                     recursionDepth,
//                                     fromIndex,
//                                     toIndex).run();
//        
//        try {
//            for (int i = 0; i != inserters.length - 1; i++) {
//                inserters[i].join();
//            } 
//        } catch (InterruptedException ex) {
//            throw new IllegalStateException(
//                    "Something happened with multithreading. Message: " +
//                            ex.getMessage());
//        }
//        
//        if (recursionDepth == LEAST_SIGNIFICANT_BYTE_INDEX) {
//            // No where to recur.
//            return;
//        }
//        
//        int numberOfNonEmptyBuckets = 0;
//        
//        for (int i : bucketSizeMap) {
//            if (i != 0) {
//                numberOfNonEmptyBuckets++;
//            }
//        }
//        
//        final int spawnDegree = Math.min(numberOfNonEmptyBuckets, threads);
//        IntArray[] bucketIndexListArray = new IntArray[spawnDegree];
//        
//        for (int i = 0; i != spawnDegree; i++) { // TODO: lower bound here?
//            bucketIndexListArray[i] = new IntArray(numberOfNonEmptyBuckets);
//        }
//        
//        int[] threadCountMap = new int[spawnDegree];
//        
//        for (int i = 0; i != spawnDegree; i++) {
//            threadCountMap[i] = threads / spawnDegree;
//        }
//        
//        for (int i = 0; i != threads % spawnDegree; i++) {
//            threadCountMap[i]++;
//        }
//        
//        // 'nonEmptyBucketIndices' will store in accending order the indices of
//        // all non-empty buckets.
//        IntArray nonEmptyBucketIndices = new IntArray(numberOfNonEmptyBuckets);
//        
//        for (int i = 0; i != BUCKETS; i++) {
//            if (bucketSizeMap[i] != 0) {
//                nonEmptyBucketIndices.add(i);
//            }
//        }
//        
//        quicksortBucketIndices(nonEmptyBucketIndices.array, 
//                               0, 
//                               nonEmptyBucketIndices.size, 
//                               bucketSizeMap);
//        
//        final int optimalSubrangeLength = rangeLength / spawnDegree;
//        int listIndex = 0;
//        int packed = 0;
//        int f = 0;
//        int j = 0;
//        
//        while (j < nonEmptyBucketIndices.size()) {
//            packed += bucketSizeMap[nonEmptyBucketIndices.array[j++]];
//            
//            if (packed >= optimalSubrangeLength
//                    || j == nonEmptyBucketIndices.size()) {
//                packed = 0;
//                
//                for (int i = f; i < j; i++) {
//                    bucketIndexListArray[listIndex]
//                            .add(nonEmptyBucketIndices.array[i]);
//                }
//                
//                listIndex++;
//                f = j;
//            };
//        }
//        
//        LongTask[][] taskMatrix = new LongTask[spawnDegree][];
//        
//        for (int threadIndex = 0; threadIndex != spawnDegree; threadIndex++) {
//            taskMatrix[threadIndex] =
//                    new LongTask[bucketIndexListArray[threadIndex].size];
//            
//            for (int i = 0; i != bucketIndexListArray[threadIndex].size; i++) {
//                taskMatrix[threadIndex][i] = 
//                        new LongTask(targetArray,
//                                     sourceArray,
//                                     targetArrayOffset,
//                                     sourceArrayOffset,
//                                     threadCountMap[threadIndex],
//                                     recursionDepth + 1,
//                                     startIndexMap[i],
//                                     startIndexMap[i] + bucketSizeMap[i]);
//            }
//        }
//        
//        LongSorterThread[] sorters = new LongSorterThread[spawnDegree];
//
//        for (int i = 0; i != spawnDegree - 1; i++) {
//            sorters[i] = new LongSorterThread(taskMatrix[i]);
//            sorters[i].start();
//        }
//        
//        new LongSorterThread(taskMatrix[spawnDegree - 1]).run();
//        
//        try {
//            for (int i = 0; i != spawnDegree - 1; i++) {
//                sorters[i].join();
//            }
//        } catch (final InterruptedException ex) {
//            throw new IllegalStateException(
//                    "Something happened with multithreading. Message: " +
//                            ex.getMessage());
//        } 
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
    static final void sortImpl(final long[] sourceArray,
                               final long[] targetArray,
                               final int auxiliaryBufferOffset,
                               final int recursionDepth,
                               final int sourceArrayFromIndex,
                               final int sourceArrayToIndex) {
        final int rangeLength = sourceArrayToIndex - sourceArrayFromIndex;
        
        if (rangeLength <= /* QUICKSORT_THRESHOLD */ 1) {
            if (recursionDepth % 2 == 0) {
                // 'sourceArray' is the actual input array.
                quicksort(sourceArray,
                          sourceArrayFromIndex,
                          sourceArrayToIndex);
            } else {
                // 'sourceArray' is actually the auxiliary buffer, so sort the
                // 'targetArray'. 
                quicksort(targetArray,
                          sourceArrayFromIndex, // - sourceArrayOffset,
                          sourceArrayToIndex  //  - sourceArrayOffset
                );
                
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
            final int bucketIndex = getBucketIndex(sourceArray[i], 
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
            for (int i = sourceArrayFromIndex; 
                     i != sourceArrayToIndex;
                     i++) {
                final int bucketIndex = getBucketIndex(sourceArray[i],
                                                       recursionDepth);
                final int elementIndex = startIndexMap[bucketIndex] +
                                          processedMap[bucketIndex] -
                                          auxiliaryBufferOffset;
                processedMap[bucketIndex]++;
                targetArray[elementIndex] = sourceArray[i]; 
            } 
        } else {
            for (int i = sourceArrayFromIndex; 
                     i != sourceArrayToIndex;
                     i++) {
                final int bucketIndex = getBucketIndex(sourceArray[i],
                                                       recursionDepth);
                final int elementIndex = startIndexMap[bucketIndex] +
                                          processedMap[bucketIndex] +
                                          auxiliaryBufferOffset;
                processedMap[bucketIndex]++;
                targetArray[elementIndex] = sourceArray[i];
        }
        
        if (recursionDepth % 2 == 0) {
            for (int i = 0; i != BUCKETS; i++) {
                if (bucketSizeMap[i] != 0) {
                    // We translate each index 'i' in 'sourceArray' to 
                    // an index 'j' in 'targetArray'. In other words,
                    // 'j = i - auxiliaryBufferOffset'.
                    sortImpl(targetArray,
                             sourceArray,
                             auxiliaryBufferOffset,
                             recursionDepth + 1,
                             startIndexMap[i] - auxiliaryBufferOffset,
                             startIndexMap[i] - auxiliaryBufferOffset 
                                              + bucketSizeMap[i]); 
                }
            }
        } else {
            for (int i = 0; i != BUCKETS; i++) {
                if (bucketSizeMap[i] != 0) {
                    // We translate each index 'i' in 'sourceArray' to 
                    // an index 'j' in 'targetArray'. In other words,
                    // 'j = i + auxiliaryBufferOffset'. Unlike above, note the
                    // minus signs.
                    sortImpl(targetArray,
                             sourceArray,
                             auxiliaryBufferOffset,
                             recursionDepth + 1,
                             startIndexMap[i] + auxiliaryBufferOffset,
                             startIndexMap[i] + auxiliaryBufferOffset 
                                              + bucketSizeMap[i]); 
                }
            }
        }
//        
//        int startIndex;
//        int endIndex;
//        
//        if (recursionDepth % 2 == 0) {
//            startIndex = sourceArrayFromIndex;
//            endIndex   = sourceArrayToIndex;
//        } else {
//            startIndex = sourceArrayFromIndex - auxiliaryBufferOffset;
//            endIndex   = sourceArrayToIndex - auxiliaryBufferOffset;
//        }
//        
//        for (int i = startIndex; i != endIndex; i++) {
//            int index = getBucketIndex(sourceArray[i], recursionDepth);
//            bucketSizeMap[index]++;
//        }
//        
//        startIndexMap[0] = sourceArrayFromIndex;
//                fromIndex -
//                (recursionDepth % 2 == 1 ? sourceArrayOffset : 0);
//        
//        for (int i = 1; i != BUCKETS; i++) {
//            startIndexMap[i] = startIndexMap[i - 1] + bucketSizeMap[i - 1];
//        }
//        
//        for (int i = startIndex; i != endIndex; i++) {
//            final int index = getBucketIndex(sourceArray[i], recursionDepth);
//            targetArray[startIndexMap[index] + processedMap[index]++] = 
//                    sourceArray[i];
//        }
//        
//        if (recursionDepth == LEAST_SIGNIFICANT_BYTE_INDEX) {
//            return;
//        }
//        
//        for (int i = 0; i != BUCKETS; i++) {
//            if (bucketSizeMap[i] != 0) {
//                sortImpl(targetArray,
//                         sourceArray,
//                         auxiliaryBufferOffset,
//                         recursionDepth + 1,
//                         startIndexMap[i],
//                         startIndexMap[i] + bucketSizeMap[i]);
//            }
//        }
    }
    
    /////////////////////////////////////////////////////////////////////  /////
     // Auxiliary classes                                                    //
    ////  //////////////////////////////////////////////////////////////////////
    
    /**
     * Implements a thread type that counts the size of each bucket 0, 1, ...,
     * 255 in the range {@code array[fromIndex], ..., array[toIndex - 1]}.
     */
    private static final class LongBucketCountingThread extends Thread {
        
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
         * The recursion depth or in other words the index of the byte being
         * considered, numeration starting from the most significant byte of
         * each {@code long} value.
         */
        private final int recursionDepth;
        
        /**
         * The index of the first element in the range.
         */
        private final int fromIndex;
        
        /**
         * The least index past the last element in the range.
         */
        private final int toIndex;
        
        /**
         * Constructs the bucket size counting thread.
         * 
         * @param array          the array containing the target range.
         * @param recursionDepth the recursion depth.
         * @param fromIndex      the starting, inclusive index.
         * @param toIndex        the ending, exclusive index.
         */
        LongBucketCountingThread(long[] array,
                                 int recursionDepth,
                                 int fromIndex,
                                 int toIndex) {
            this.localBucketSizeMap = new int[BUCKETS];
            this.array = array;
            this.recursionDepth = recursionDepth;
            this.fromIndex = fromIndex;
            this.toIndex = toIndex;
        }
        
        /**
         * Counts the bucket size of each bucket in the target range.
         */
        @Override
        public void run() {
            for (int i = fromIndex; i != toIndex; i++) {
                localBucketSizeMap[getBucketIndex(array[i], recursionDepth)]++;
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
                int bucketIndex = getBucketIndex(sourceArray[i], 
                                                 recursionDepth);
                targetArray[startIndexMap[bucketIndex] + 
                            processedMap [bucketIndex] + targetArrayOffset] =
                        sourceArray[i];
            }
        }
    }
    
    private static final class LongSorterThread extends Thread {
    
        private final LongTask[] taskList;
        
        LongSorterThread(LongTask[] taskList) {
            this.taskList = taskList;
        }
        
        @Override
        public void run() {
            for (LongTask longTask : taskList) {
                if (longTask.threads > 1) {
                    parallelSortImpl(longTask.sourceArray,
                                     longTask.targetArray,
                                     longTask.sourceArrayOffset,
                                     longTask.threads,
                                     longTask.recursionDepth,
                                     longTask.fromIndex,
                                     longTask.toIndex);
                } else {
                    sortImpl(longTask.sourceArray,
                             longTask.targetArray,
                             longTask.sourceArrayOffset,
                             longTask.recursionDepth,
                             longTask.fromIndex,
                             longTask.toIndex);
                }
            }
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
    
    private static final int getBucketIndex(final long key, 
                                            final int recursionDepth) {
        final int bitShift = 64 - (recursionDepth + 1) * Long.BYTES;
        return (int)((key ^ SIGN_MASK) >>> bitShift) & BUCKET_MASK;
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
}