package net.coderodde.util;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

/**
 * This class contains an implementation of a parallel sorting routine for 
 * parallel arrays as a set of static class methods. The parallel arrays are 
 * implemented as arrays of objects of class {@link net.coderodde.util.Entry}, 
 * which contain the actual
 * <code>long</code> key, and the reference to the satellite data comprising
 * whatever is associated with the aforementioned key.
 * 
 * The actual algorithm is based on MSD (most-significant digit) radix sort, 
 * which starts by sorting the array by the most-significant <b>bytes</b>. After
 * the data is sorted (only by most significant bytes), the algorithm recurs to
 * sort each bucket represented by a most-significant byte using the second
 * most-significant byte of keys, and continues in this fashion until the range
 * contains no more than a specified amount of elements, in which case it is
 * sorted using a bottom-up merge sort, or the algorithm reaches the least-
 * significant byte so there is no more bytes to process.
 * 
 * The underlying implementation takes into account the sign bit of each key, so 
 * that all entries whose keys' sign bit is set will precede all the entries 
 * whose keys' sign bit is unset. Also, the underlying implementation sorts
 * <b>stably</b> any data (i.e., it preserves the relative order of entries 
 * with equal keys).
 * 
 * @author Rodion Efremov
 * @version 2014.11.29
 */
public class Arrays {

    /**
     * The amount of buckets considered at each invocation. 
     * As <tt>log_2(256) = 8</tt>, this implies that the complexity of 
     * sequential radix sort is <tt>O(kN)</tt>, where <tt>k</tt> is 
     * between 1 and 8, inclusively.
     */
    private static final int BUCKETS = 256;
    
    /**
     * The amount of bits considered at each invocation.
     */
    private static final int BITS_PER_BYTE = 8;
    
    /**
     * The amount of bits to be shifted to the right as to obtain a bucket 
     * index, which always has values between 0 and 255, inclusively.
     */
    private static final int RIGHT_SHIFT_AMOUNT = 56;
    
    /**
     * The index of the most significant byte. As the following radix sort
     * sorts by <code>long</code> keys, the index of the most significant byte
     * is 7.
     */
    private static final int MOST_SIGNIFICANT_BYTE_INDEX = 7;
    
    /**
     * The minimum amount of entries to sort for a thread (2^16).
     */
    private static final int THREAD_THRESHOLD = 65536;
    
    /**
     * The maximum amount of entries to sort using a merge sort.
     */
    private static final int MERGESORT_THRESHOLD = 4096;
    
    /**
     * This constant is used only while processing the most significant bytes,
     * and it denotes the "least" bucket which contains keys with the least
     * value below zero.
     */
    private static final int LEAST_SIGNED_BUCKET_INDEX = 128;
    
    /**
     * Sorts the entire array containing the 
     * entries ({@link net.coderodde.util.Entry}).
     * 
     * @param <E> the type of actual satellite data.
     * @param array the array to sort.
     */
    public static <E> void parallelSort(final Entry<E>[] array) {
        parallelSort(array, 0, array.length);
    }
    
    /**
     * Sorts the range <tt>[fromIndex, toIndex)</tt> of the given array.
     * 
     * @param <E> the type of actual satellite data.
     * @param array the array containing requested range.
     * @param fromIndex the least, inclusive index of the range to sort.
     * @param toIndex the exclusive index specifying the right end of the range.
     */
    public static <E> void parallelSort(final Entry<E>[] array,
                                        final int fromIndex,
                                        final int toIndex) {
        if (toIndex - fromIndex < 2) {
            return;
        }
        
        final Entry<E>[] buffer = array.clone();
        parallelSortImplTopLevel(array, buffer, fromIndex, toIndex);
    }
    
    /**
     * This method sorts the data by the most significant byte, taking the 
     * possible set sign bits into consideration.
     * 
     * @param <E> The type of entries' satellite data.
     * @param array the array holding the requested range.
     * @param buffer the auxiliary copy of <code>array</code>. 
     * @param fromIndex the least, inclusive index of the range to sort.
     * @param toIndex the ending index of the range to sort, exclusive.
     */
    private static <E> void parallelSortImplTopLevel(final Entry<E>[] array,
                                                     final Entry<E>[] buffer,
                                                     final int fromIndex,
                                                     final int toIndex) {
        // The amount of elements in the requested range.
        final int RANGE_LENGTH = toIndex - fromIndex;
        
        if (RANGE_LENGTH <= MERGESORT_THRESHOLD) {
            // Once here, the range is too small, use merge sort.
            
            // The amount of merge passes needed to sort the input range.
            final int PASSES = (int)(Math.ceil(Math.log(RANGE_LENGTH) /
                                               Math.log(2)));
            
            // Here, both 'array' and 'buffer' are identical in content.
            if ((PASSES & 1) == 0) {
                // Once here, there will be an even amount of merge passes, so
                // it makes sense to pass 'array' as the source array so that
                // the actual sorted data ends up in it, so there is no need
                // to copy the sorted range from 'buffer' to 'array'.
                mergesort(array, buffer, fromIndex, toIndex);
            } else {
                // A symmetric case: sort using 'buffer' as the source array
                // so that the sorted data ends up in 'array' and we don't have
                // to do additional copying.
                mergesort(buffer, array, fromIndex, toIndex);
            }
            
            return;
        }
        
        final int THREADS = Math.min(Runtime.getRuntime().availableProcessors(),
                                     RANGE_LENGTH / THREAD_THRESHOLD);
        
        if (THREADS < 2) {
            // No threads available, just use the sequential version so as to
            // have slighly better constant factors.
            sortTopImpl(array, buffer, fromIndex, toIndex);
            return;
        }
        
        // Create the threads counting sizes of each present bucket.
        final BucketSizeCounter[] counters = new BucketSizeCounter[THREADS];
        final int SUB_RANGE_LENGTH = RANGE_LENGTH / THREADS;
        int start = fromIndex;
        
        for (int i = 0; i != THREADS - 1; ++i, start += SUB_RANGE_LENGTH) {
            counters[i] = new BucketSizeCounter<>(array,
                                                  MOST_SIGNIFICANT_BYTE_INDEX,
                                                  start,
                                                  start + SUB_RANGE_LENGTH);
            counters[i].start();
        }
        
        // The last one will be run in this thread while the other run in 
        // parallel.
        counters[THREADS - 1] = 
                new BucketSizeCounter<>(array,
                                        MOST_SIGNIFICANT_BYTE_INDEX,
                                        start,
                                        toIndex);
        
        counters[THREADS - 1].run();
        
        try {
            for (int i = 0; i != THREADS - 1; ++i) {
                counters[i].join();
            }
        } catch (final InterruptedException ie) {
            ie.printStackTrace();
            return;
        }
        
        final int[] bucketSizeMap = new int[BUCKETS];
        final int[] startIndexMap = new int[BUCKETS];
        
        // Create the global bucket size map by summing the sizes of each
        // bucket in each thread.
        for (int i = 0; i != THREADS; ++i) {
            for (int j = 0; j != BUCKETS; ++j) {
                bucketSizeMap[j] += counters[i].localBucketSizeMap[j];
            }
        }
        
        // This is the special treatment of buckets, the one that should end up
        // in the least indices of the range to sort has as its byte value
        // '128', which is the least value with the sign bit set.
        startIndexMap[LEAST_SIGNED_BUCKET_INDEX] = fromIndex;
        
        for (int i = LEAST_SIGNED_BUCKET_INDEX + 1; i != BUCKETS; ++i) {
            startIndexMap[i] = startIndexMap[i - 1] +
                               bucketSizeMap[i - 1];
        }
        
        startIndexMap[0] = startIndexMap[BUCKETS - 1] + 
                           bucketSizeMap[BUCKETS - 1];
        
        for (int i = 1; i != LEAST_SIGNED_BUCKET_INDEX; ++i) {
            startIndexMap[i] = startIndexMap[i - 1] +
                               bucketSizeMap[i - 1];
        }
        
        // Here, the 'startIndexMap' is initialized.
        // Next, create the threads for inserting the buckets in the scanned
        // array to another one. This is a time-memory trade-off.
        final BucketInserter<E>[] inserters = new BucketInserter[THREADS - 1];
        // This map is used to make the inserter threads independent of each
        // other. For each thread 0, 1, ..., P - 1, thread 'i' will obtain
        // the array 'processedMaps[i]', and each of thea arrays will have 
        // 'BUCKETS' components. Each component denotes how much entries in the 
        // target array a thread 'i' should skip so as to not interfere with the 
        // range processed by the thread with index 'i - 1'.
        
        // For instance, assume there is 4 threads and bucket 0xAB is of length
        // 12 entries, 2 leftmost entries where scanned by counter 0,
        // 3 by counter 1, 4 by counter 2, 3 by counter 3. Also the starting
        // index of the bucket is j; now, inserter thread 0 will have 0 as its
        // processedMaps[0xAB] and will insert only to range [j, j + 2), 
        // inserter thread 1 will have 2 as its processedMaps[0xAB] and will work
        // on range [j + 2, j + 5), inserter thread 2 [j + 5, j + 9), and 
        // inserter 3 [j + 9, j + 12).
        final int[][] processedMaps = new int[THREADS][BUCKETS];
        
        for (int i = 1; i != THREADS; ++i) {
            int[] partialBucketSizeMap = counters[i - 1].localBucketSizeMap;
            
            for (int j = 0; j != BUCKETS; ++j) {
                processedMaps[i][j] = 
                        processedMaps[i - 1][j] + partialBucketSizeMap[j];
            }
        }
        
        int startIndex = fromIndex;
        
        // Create the inserter threads and spawn them.
        for (int i = 0; i != THREADS - 1; ++i, startIndex += SUB_RANGE_LENGTH) {
            inserters[i] =
                    new BucketInserter<>(startIndexMap,
                                         processedMaps[i],
                                         array,
                                         buffer,
                                         MOST_SIGNIFICANT_BYTE_INDEX,
                                         startIndex,
                                         startIndex + SUB_RANGE_LENGTH);
            inserters[i].start();
        }
        
        // Run the last inserter in this thread.
        new BucketInserter<>(startIndexMap,
                             processedMaps[THREADS - 1],
                             array,
                             buffer,
                             MOST_SIGNIFICANT_BYTE_INDEX,
                             startIndex,
                             toIndex).run();
        
        try {
            for (int i = 0; i != THREADS - 1; ++i) {
                inserters[i].join();
            }
        } catch (final InterruptedException ie) {
            ie.printStackTrace();
            return;
        }
        
        int nonEmptyBucketAmount = 0;
        
        for (int i : bucketSizeMap) {
            if (i != 0) {
                ++nonEmptyBucketAmount;
            }
        }
        
        // Decide how much threads to allocate. 
        final int SPAWN_DEGREE = Math.min(nonEmptyBucketAmount, THREADS);
        final List<Integer>[] bucketIndexListArray = new List[SPAWN_DEGREE];
        
        for (int i = 0; i != SPAWN_DEGREE; ++i) {
            bucketIndexListArray[i] = new ArrayList<>(nonEmptyBucketAmount);
        }
        
        // Each component i of the array below will denote how much threads to
        // "inherit" for thread i.
        final int[] threadCountMap = new int[SPAWN_DEGREE];
        
        for (int i = 0; i != SPAWN_DEGREE; ++i) {
            threadCountMap[i] = THREADS / SPAWN_DEGREE;
        }
        
        for (int i = 0; i != THREADS % SPAWN_DEGREE; ++i) {
            ++threadCountMap[i];
        }
        
        final Sorter[] sorters = new Sorter[SPAWN_DEGREE];
        final List<Integer> bucketIndexList = new ArrayList<>(BUCKETS);
        
        for (int i = 0; i != BUCKETS; ++i) {
            if (bucketSizeMap[i] > 0) {
                bucketIndexList.add(i);
            }
        }
        
        // bucketIndexList is descending in bucket size. This is a pathetic way
        // of trying to balance the workload between the threads. Or we could
        // hang the CPU while indulging ourselves in running some optimal, 
        // NP-hard load-balancing routine.
        Collections.sort(bucketIndexList, 
                         new BucketSizeComparator(bucketSizeMap));
        
        final int OPTIMAL_SUBRANGE_LENGTH = RANGE_LENGTH / SPAWN_DEGREE;
        // This list will describe the subsorting tasks for each thread.
        final List<List<Task<E>>> tll = new ArrayList<>(SPAWN_DEGREE);
        int bucketIndex = 0;
        
        for (int i = 0; i != SPAWN_DEGREE; ++i) {
            final List<Task<E>> tl = new ArrayList<>();
            int packed = 0;
            
            while (packed < OPTIMAL_SUBRANGE_LENGTH && bucketIndex < BUCKETS) {
                tl.add(new Task<>(buffer,
                                  array,
                                  threadCountMap[SPAWN_DEGREE - 1 - i],
                                  MOST_SIGNIFICANT_BYTE_INDEX - 1,
                                  startIndexMap[bucketIndex],
                                  startIndexMap[bucketIndex] + // Note the '+'!
                                  bucketSizeMap[bucketIndex]));
                packed += bucketSizeMap[bucketIndex];
                ++bucketIndex;
            }
            
            tll.add(tl);
        }
        
        for (int i = 0; i != SPAWN_DEGREE - 1; ++i) {
            sorters[i] = new Sorter<>(tll.get(i));
            sorters[i].start();
        }
        
        new Sorter<>(tll.get(SPAWN_DEGREE - 1)).run();
        
        try {
            for (int i = 0; i != SPAWN_DEGREE - 1; ++i) {
                sorters[i].join();
            }
        } catch (final InterruptedException ie) {
            ie.printStackTrace();
            return;
        }
    }
    
    /**
     * This static method sorts sequentially an entry array by most-significant
     * bytes.
     * 
     * @param <E> the type of satellite data of each entry.
     * @param array the actual array to sort.
     * @param buffer the auxiliary buffer.
     * @param fromIndex the least index of the range to sort.
     * @param toIndex the index one past the last index of the range to sort.
     */
    private static final <E> void sortTopImpl(final Entry<E>[] array,
                                              final Entry<E>[] buffer,
                                              final int fromIndex,
                                              final int toIndex) {
        
        final int[] bucketSizeMap = new int[BUCKETS];
        final int[] startIndexMap = new int[BUCKETS];
        final int[] processedMap  = new int[BUCKETS];
        
        // Determine the size of each bucket.
        for (int i = fromIndex; i < toIndex; ++i) {
            bucketSizeMap[(int)(array[i].key >>> RIGHT_SHIFT_AMOUNT)]++;
        }
        
        // BEGIN: Special sign bit magic.
        startIndexMap[LEAST_SIGNED_BUCKET_INDEX] = fromIndex;
        
        for (int i = LEAST_SIGNED_BUCKET_INDEX + 1; i != BUCKETS; ++i) {
            startIndexMap[i] = startIndexMap[i - 1] +
                               bucketSizeMap[i - 1];
        }
        
        startIndexMap[0] = startIndexMap[BUCKETS - 1] +
                           bucketSizeMap[BUCKETS - 1];
        
        for (int i = 1; i != LEAST_SIGNED_BUCKET_INDEX; ++i) {
            startIndexMap[i] = startIndexMap[i - 1] +
                               bucketSizeMap[i - 1];
        }
        // END: Special sign bit magic.
        
        // Insert elements into their respective buckets in the buffer array.
        for (int i = fromIndex; i < toIndex; ++i) {
            final Entry<E> e = array[i];
            final int index = (int)(e.key >>> RIGHT_SHIFT_AMOUNT);
            buffer[startIndexMap[index] + processedMap[index]++] = e;
        }
        
        // Recur to sort all non-empty buckets.
        for (int i = 0; i != BUCKETS; ++i) {
            if (bucketSizeMap[i] != 0) {
                sortImpl(buffer,
                         array,
                         MOST_SIGNIFICANT_BYTE_INDEX - 1,
                         startIndexMap[i],
                         startIndexMap[i] + bucketSizeMap[i]);
            }
        }
    }
    
    /**
     * This static method sorts the entry array by bytes that are not
     * most-significant.
     * 
     * @param <E> the type of satellite data in the entry array.
     * @param source the source array.
     * @param target the target array.
     * @param byteIndex the index of the byte to use as the sorting key. 0 
     * represents the least-significant byte.
     * @param fromIndex the least index of the range to sort.
     * @param toIndex the index one past the greatest index of the range to 
     * sort.
     */
    private static final <E> void sortImpl(final Entry<E>[] source,
                                           final Entry<E>[] target,
                                           final int byteIndex,
                                           final int fromIndex,
                                           final int toIndex) {
        // Try merge sort.
        if (toIndex - fromIndex <= MERGESORT_THRESHOLD) {
            // If 'even' is true, the sorted ranged ended up in 'source'.
            final boolean even = mergesort(source, target, fromIndex, toIndex);
            
            if (even) {
                // source contains the sorted bucket.
                if ((byteIndex & 1) == 0) {
                    // byteIndex = 6, 4, 2, 0.
                    // source is buffer, copy to target.
                    System.arraycopy(source,
                                     fromIndex, 
                                     target,
                                     fromIndex, 
                                     toIndex - fromIndex);
                }
            } else {
                // target contains the sorted bucket.
                if ((byteIndex & 1) == 1) {
                    // byteIndex = 5, 3, 1.
                    // target is buffer, copy to source.
                    System.arraycopy(target, 
                                     fromIndex,
                                     source, 
                                     fromIndex, 
                                     toIndex - fromIndex);
                }
            }
            
            return;
        }
        
        final int[] bucketSizeMap = new int[BUCKETS];
        final int[] startIndexMap = new int[BUCKETS];
        final int[] processedMap  = new int[BUCKETS];
        
        // We need this as to get rid of the bits on the left from the byte we
        // are interesed in.
        final int LEFT_SHIFT_AMOUNT =
                BITS_PER_BYTE * (MOST_SIGNIFICANT_BYTE_INDEX - byteIndex);
        
        // Compute the size of each bucket.
        for (int i = fromIndex; i < toIndex; ++i) {
            bucketSizeMap[(int)((source[i].key << LEFT_SHIFT_AMOUNT)
                                             >>> RIGHT_SHIFT_AMOUNT)]++;
        }
        
        // Initialize the start index map.
        startIndexMap[0] = fromIndex;
        
        // Compute the start index map in its entirety.
        for (int i = 1; i != BUCKETS; ++i) {
            startIndexMap[i] = startIndexMap[i - 1] +
                               bucketSizeMap[i - 1];
        }
        
        // Insert the entries from 'source' into their respective 'target'.
        for (int i = fromIndex; i < toIndex; ++i) {
            final Entry<E> e = source[i];
            final int index = (int)((e.key << LEFT_SHIFT_AMOUNT)
                                         >>> RIGHT_SHIFT_AMOUNT);
            target[startIndexMap[index] + processedMap[index]++] = e;
        }
        
        if (byteIndex == 0) {
            // There is nowhere to recur, return.
            return;
        }
        
        // Recur to sort each bucket.
        for (int i = 0; i != BUCKETS; ++i) {
            if (bucketSizeMap[i] != 0) {
                sortImpl(target,
                         source,
                         byteIndex - 1,
                         startIndexMap[i],
                         startIndexMap[i] + bucketSizeMap[i]);
            }
        }
    }
    
    /**
     * Sorts the range <code>[fromIndex, toIndex)</code> between the arrays
     * <code>source</code> and <code>target</code>.
     * 
     * @param <E> the type of entries' satellite data.
     * @param source the source array; the data to sort is assumed to be in this
     * array.
     * @param target acts as an auxiliary array.
     * @param fromIndex the least component index of the range to sort.
     * @param toIndex <code>toIndex - 1</code> is the index of the rightmost
     * component in the range to sort.
     * @return <code>true</code> if there was an even amount of merge passes,
     * which implies that the sorted range ended up in <code>source</code>.
     * Otherwise <code>false</code> is returned, and the sorted range ended up
     * in the array <code>target</code>.
     */
    private static final <E> boolean mergesort(final Entry<E>[] source,
                                               final Entry<E>[] target,
                                               final int fromIndex,
                                               final int toIndex) {
        final int RANGE_LENGTH = toIndex - fromIndex;
        
        Entry<E>[] s = source;
        Entry<E>[] t = target;
        
        int passes = 0;
        
        for (int width = 1; width < RANGE_LENGTH; width <<= 1) {
            ++passes;
            int c = 0;
            
            for (; c < RANGE_LENGTH / width; c += 2) {
                int left = fromIndex + c * width;
                int right = left + width;
                int i = left;
                
                final int leftBound = right;
                final int rightBound = Math.min(toIndex, right + width);
                
                while (left < leftBound && right < rightBound) {
                    t[i++] = s[right].key < s[left].key ?
                             s[right++] :
                             s[left++];
                }
                
                while (left < leftBound)   { t[i++] = s[left++]; }
                while (right < rightBound) { t[i++] = s[right++]; }
            }
            
            if (c * width < RANGE_LENGTH) {
                for (int i = fromIndex + c * width; i < toIndex; ++i) {
                    t[i] = s[i];
                }
            }
            
            final Entry<E>[] tmp = s;
            s = t;
            t = tmp;
        }
        
        return (passes & 1) == 0;
    }
    
    /**
     * This thread is responsible for counting bucket sizes within an array
     * range.
     * 
     * @param <E> the type of entries' satellite data.
     */
    private static final class BucketSizeCounter<E> extends Thread {
        
        /**
         * The actual map holding the size of bucket <code>i</code> in 
         * <code>localBucketSizeMap[i]</code>.
         */
        int[] localBucketSizeMap;
        
        /**
         * The array of entries holding the range of entries to process.
         */
        private final Entry<E>[] source;
        
        /**
         * Specifies the index of the byte to use as the bucket key. 0 
         * corresponds to the least-significant byte.
         */
        private final int byteIndex;
        
        /**
         * Specifies the least index of the range to process.
         */
        private final int fromIndex;
        
        /**
         * Specifies the index past one element in the range to process.
         */
        private final int toIndex;
        
        BucketSizeCounter(final Entry<E>[] source,
                          final int byteIndex,
                          final int fromIndex,
                          final int toIndex) {
            this.source = source;
            this.byteIndex = byteIndex;
            this.fromIndex = fromIndex;
            this.toIndex = toIndex;
        }
        
        @Override
        public void run() {
            this.localBucketSizeMap = new int[BUCKETS];
            
            if (byteIndex == MOST_SIGNIFICANT_BYTE_INDEX) {
                // A mild optimization.
                for (int i = fromIndex; i < toIndex; ++i) {
                    ++localBucketSizeMap[(int)(source[i].key
                                         >>> RIGHT_SHIFT_AMOUNT)];
                }
            } else {
                final int LEFT_SHIFT_AMOUNT = 
                        BITS_PER_BYTE * 
                        (MOST_SIGNIFICANT_BYTE_INDEX - byteIndex);
                
                for (int i = fromIndex; i < toIndex; ++i) {
                    ++localBucketSizeMap[(int)
                                         ((source[i].key << LEFT_SHIFT_AMOUNT)
                                         >>> RIGHT_SHIFT_AMOUNT)];
                }
            }
        }
    }
    
    /**
     * This thread is responsible for inserting the entries from one array into 
     * the respective buckets.
     * 
     * @param <E> the type of the satellite data in entries.
     */
    private static final class BucketInserter<E> extends Thread {
        
        /**
         * Maps a bucket byte to its least index.
         */
        private final int[] startIndexMap;
        
        /**
         * Used as to make bucket inserter threads independent. 
         */
        private final int[] processedMap;
        
        /**
         * The array holding the source range.
         */
        private final Entry<E>[] source;
        
        /**
         * The array holding the resulting buckets.
         */
        private final Entry<E>[] target;
        
        /**
         * Specifies the index of the byte to use as the bucket key. 0 
         * corresponds to the least-significant byte.
         */
        private final int byteIndex;
        
        /**
         * The least index of the range to process.
         */
        private final int fromIndex;
        
        /**
         * The index on past the last index of the range to process.
         */
        private final int toIndex;
        
        BucketInserter(final int[] startIndexMap,
                       final int[] processedMap,
                       final Entry<E>[] source,
                       final Entry<E>[] target,
                       final int byteIndex,
                       final int fromIndex,
                       final int toIndex) {
            this.startIndexMap = startIndexMap;
            this.processedMap = processedMap;
            this.source = source;
            this.target = target;
            this.byteIndex = byteIndex;
            this.fromIndex = fromIndex;
            this.toIndex = toIndex;
        }
        
        @Override
        public void run() {
            if (byteIndex == MOST_SIGNIFICANT_BYTE_INDEX) {
                // Mild optimization.
                for (int i = fromIndex; i < toIndex; ++i) {
                    final Entry<E> e = source[i];
                    final int index = (int)(e.key >>> RIGHT_SHIFT_AMOUNT);
                    target[startIndexMap[index] + processedMap[index]++] = e;
                }
            } else {
                final int LEFT_SHIFT_AMOUNT = BITS_PER_BYTE *
                        (MOST_SIGNIFICANT_BYTE_INDEX - byteIndex);
                
                for (int i = fromIndex; i < toIndex; ++i) {
                    final Entry e = source[i];
                    final int index = (int)((e.key << LEFT_SHIFT_AMOUNT)
                                                 >>> RIGHT_SHIFT_AMOUNT);
                    target[startIndexMap[index] + processedMap[index]++] = e;
                }
            }
        }
    }
    
    /**
     * This thread sorts the buckets recurring as a sequential routine, or 
     * issues a parallel sort.
     * 
     * @param <E> the type of the entries' satellite data. 
     */
    private static final class Sorter<E> extends Thread {
        
        /**
         * The list containing the task, where each task corresponds to a bucket
         * on some recursion level.
         */
        private final List<Task<E>> taskList;
        
        Sorter(final List<Task<E>> taskList) {
            this.taskList = taskList;
        }
        
        @Override
        public void run() {
            for (final Task task : taskList) {
                // Choose parallel or sequential.
                if (task.threads > 1) {
                    parallelSortImpl(task.source,
                                     task.target,
                                     task.threads,
                                     task.byteIndex,
                                     task.fromIndex,
                                     task.toIndex);
                } else {
                    sortImpl(task.source,
                             task.target,
                             task.byteIndex,
                             task.fromIndex,
                             task.toIndex);
                }
            }
        }
    }
    
    /**
     * This static method implements the parallel radix sort on bytes that are
     * not most-significant. This of use in situations where sorting by the 
     * previous byte produced less buckets than threads.
     * 
     * @param <E> the type of the satellite data in entries.
     * @param source the array holding the input range.
     * @param target the array in which elements will be placed in their
     * respective buckets.
     * @param threads the amount of threads this call may utilize.
     * @param byteIndex the index of the byte to use as the sorting key. 0
     * denotes the least-significant byte.
     * @param fromIndex the least index of the input range.
     * @param toIndex the index one past the last of the input range.
     */
    private static final <E> void parallelSortImpl(final Entry<E>[] source,
                                                   final Entry<E>[] target,
                                                   final int threads,
                                                   final int byteIndex,
                                                   final int fromIndex,
                                                   final int toIndex) {
        final int RANGE_LENGTH = toIndex - fromIndex;
        
        if (RANGE_LENGTH <= MERGESORT_THRESHOLD) {
            final boolean even = mergesort(source, target, fromIndex, toIndex);
            
            if (even) {
                // source contains the sorted bucket.
                if ((byteIndex & 1) == 0) {
                    // byteIndex = 6, 4, 2, 0.
                    // source is buffer, copy to target.
                    System.arraycopy(source,
                                     fromIndex, 
                                     target,
                                     fromIndex, 
                                     toIndex - fromIndex);
                }
            } else {
                // target contains the sorted bucket.
                if ((byteIndex & 1) == 1) {
                    // byteIndex = 5, 3, 1.
                    // target is buffer, copy to source.
                    System.arraycopy(target, 
                                     fromIndex,
                                     source, 
                                     fromIndex, 
                                     toIndex - fromIndex);
                }
            }
            
            return;
        }
        
        if (threads < 2) {
            System.out.println("838");
            sortImpl(source, target, byteIndex, fromIndex, toIndex);
            return;
        }
        
        // Create the bucket size counter threads.
        final BucketSizeCounter[] counters = new BucketSizeCounter[threads];
        final int SUB_RANGE_LENGTH = RANGE_LENGTH / threads;
        int start = fromIndex;
        
        for (int i = 0; i != threads - 1; ++i, start += SUB_RANGE_LENGTH) {
            counters[i] = new BucketSizeCounter<>(source,
                                                  byteIndex,
                                                  start,
                                                  start + SUB_RANGE_LENGTH);
            counters[i].start();
        }
        
        counters[threads - 1] = 
                new BucketSizeCounter<>(source,
                                        byteIndex,
                                        start,
                                        toIndex);
        
        // Run the last counter in this thread while other are already on their
        // way.
        counters[threads - 1].run();
        
        try {
            for (int i = 0; i != threads - 1; ++i) {
                counters[i].join();
            }
        } catch (final InterruptedException ie) {
            ie.printStackTrace();
            return;
        }
        
        final int[] bucketSizeMap = new int[BUCKETS];
        final int[] startIndexMap = new int[BUCKETS];
        
        // Count the size of each processed bucket.
        for (int i = 0; i != threads; ++i) {
            for (int j = 0; j != BUCKETS; ++j) {
                bucketSizeMap[j] += counters[i].localBucketSizeMap[j];
            }
        }
        
        // Prepare the starting indices of each bucket.
        startIndexMap[0] = fromIndex;
        
        for (int i = 1; i != BUCKETS; ++i) {
            startIndexMap[i] = startIndexMap[i - 1] +
                               bucketSizeMap[i - 1];
        }
        
        // Create the inserter threads.
        final BucketInserter<E>[] inserters = new BucketInserter[threads - 1];
        final int[][] processedMaps = new int[threads][BUCKETS];
        
        // See the docs for parallelSortImplTopLevel for the description of what
        // happens here.
        for (int i = 1; i != threads; ++i) {
            int[] partialBucketSizeMap = counters[i - 1].localBucketSizeMap;
            
            for (int j = 0; j != BUCKETS; ++j) {
                processedMaps[i][j] = 
                        processedMaps[i - 1][j] + partialBucketSizeMap[j];
            }
        }
        
        int startIndex = fromIndex;
        
        for (int i = 0; i != threads - 1; ++i, startIndex += SUB_RANGE_LENGTH) {
            inserters[i] =
                    new BucketInserter<>(startIndexMap,
                                         processedMaps[i],
                                         source,
                                         target,
                                         byteIndex,
                                         startIndex,
                                         startIndex + SUB_RANGE_LENGTH);
            inserters[i].start();
        }
        
        // Run the last inserter in this thread while other are on their ways.
        new BucketInserter<>(startIndexMap,
                             processedMaps[threads - 1],
                             source,
                             target,
                             byteIndex,
                             startIndex,
                             toIndex).run();
        
        try {
            for (int i = 0; i != threads - 1; ++i) {
                inserters[i].join();
            }
        } catch (final InterruptedException ie) {
            ie.printStackTrace();
            return;
        }
        
        if (byteIndex == 0) {
            // Nowhere to recur.
            return;
        }
        
        int nonEmptyBucketAmount = 0;
        
        for (int i : bucketSizeMap) {
            if (i != 0) {
                ++nonEmptyBucketAmount;
            }
        }
        
        final int SPAWN_DEGREE = Math.min(nonEmptyBucketAmount, threads);
        final List<Integer>[] bucketIndexListArray = new List[SPAWN_DEGREE];
        
        for (int i = 0; i != SPAWN_DEGREE; ++i) {
            bucketIndexListArray[i] = new ArrayList<>(nonEmptyBucketAmount);
        }
        
        final int[] threadCountMap = new int[SPAWN_DEGREE];
        
        for (int i = 0; i != SPAWN_DEGREE; ++i) {
            threadCountMap[i] = threads / SPAWN_DEGREE;
        }
        
        for (int i = 0; i != threads % SPAWN_DEGREE; ++i) {
            ++threadCountMap[i];
        }
        
        final List<Integer> nonEmptyBucketIndices = 
                new ArrayList<>(nonEmptyBucketAmount);
        
        
        for (int i = 0; i != BUCKETS; ++i) {
            if (bucketSizeMap[i] != 0) {
                nonEmptyBucketIndices.add(i);
            }
        }
        
        Collections.sort(nonEmptyBucketIndices, 
                         new BucketSizeComparator(bucketSizeMap));
        
        final int OPTIMAL_SUBRANGE_LENGTH = RANGE_LENGTH / SPAWN_DEGREE;
        int listIndex = 0;
        int packed = 0;
        int f = 0;
        int j = 0;
        
        while (j < nonEmptyBucketIndices.size()) {
            int tmp = bucketSizeMap[nonEmptyBucketIndices.get(j++)];
            packed += tmp;
            
            if (packed >= OPTIMAL_SUBRANGE_LENGTH
                    || j == nonEmptyBucketIndices.size()) {
                packed = 0;
                
                for (int i = f; i < j; ++i) {
                    bucketIndexListArray[listIndex]
                            .add(nonEmptyBucketIndices.get(i));
                }
                
                ++listIndex;
                f = j;
            }
        }
        
        final Sorter[] sorters = new Sorter[SPAWN_DEGREE];
        final List<List<Task<E>>> llt = new ArrayList<>(SPAWN_DEGREE);
        
        for (int i = 0; i != SPAWN_DEGREE; ++i) {
            final List<Task<E>> lt = new ArrayList<>();
            
            for (int idx : bucketIndexListArray[i]) {
                lt.add(new Task<>(target,
                                  source,
                                  threadCountMap[i],
                                  byteIndex - 1,
                                  startIndexMap[idx],
                                  startIndexMap[idx] + bucketSizeMap[idx]));
            }
            
            llt.add(lt);
        }
        
        for (int i = 0; i != SPAWN_DEGREE - 1; ++i) {
            sorters[i] = new Sorter<>(llt.get(i));
            sorters[i].start();
        }
        
        new Sorter<>(llt.get(SPAWN_DEGREE - 1)).run();
        
        try {
            for (int i = 0; i != SPAWN_DEGREE - 1; ++i) {
                sorters[i].join();
            }
        } catch (final InterruptedException ie) {
            ie.printStackTrace();
            return;
        }
    }
    
    /**
     * This class specifies exactly a subtask of sorting a bucket.
     * 
     * @param <E> the type of satellite data of entries.
     */
    private static final class Task<E> {
        
        /**
         * The array holding the input range.
         */
        private final Entry<E>[] source;
        
        /**
         * The array holding the place for resulting buckets.
         */
        private final Entry<E>[] target;
        
        /**
         * The amount of threads to use in this task.
         */
        private final int threads;
        
        /**
         * The index of the byte to use as the key. 0 corresponds to 
         * least-significant byte.
         */
        private final int byteIndex;
        
        /**
         * The least index of the input range.
         */
        private final int fromIndex;
        
        /**
         * The index one past the last of the input range.
         */
        private final int toIndex;
        
        Task(final Entry<E>[] source,
             final Entry<E>[] target,
             final int threads,
             final int byteIndex,
             final int fromIndex,
             final int toIndex) {
            this.source = source;
            this.target = target;
            this.threads = threads;
            this.byteIndex = byteIndex;
            this.fromIndex = fromIndex;
            this.toIndex = toIndex;
        }
    }

    /**
     * Implements the comparator, putting the larger buckets to the left of 
     * holder array.
     */
    private static final class BucketSizeComparator 
    implements Comparator<Integer> {
        private final int[] bucketSizeMap;

        BucketSizeComparator(final int[] bucketSizeMap) {
            this.bucketSizeMap = bucketSizeMap;
        }

        @Override
        public int compare(final Integer i1, final Integer i2) {
            final int sz1 = bucketSizeMap[i1];
            final int sz2 = bucketSizeMap[i2];
            return sz2 - sz1;
        }
    }
}
