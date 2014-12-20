package net.coderodde.util;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;

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
public class CoderoddeArrays {

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
     * The maximum amount of entries to sort using a merge sort.
     */
    private static final int MERGESORT_THRESHOLD = 4096;
    
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
        final int RANGE_LENGTH = toIndex - fromIndex;
        
        if (RANGE_LENGTH < 2) {
            return;
        }
        
        final Entry<E>[] buffer = array.clone();
        final int threads = Math.min(RANGE_LENGTH / THREAD_THRESHOLD, 
                                     Runtime.getRuntime()
                                            .availableProcessors());
        parallelSortImpl(array, buffer, threads, 0, fromIndex, toIndex);
    }
    
    /**
     * Checks whether the input arrays are of the same length, and have exactly
     * the same references at each component.
     * 
     * @param <E> the type of satellite data.
     * @param arrays the array of arrays.
     * 
     * @return <code>true</code> if the arrays are equal. <code>false</code>
     *         otherwise.
     */
    public static final <E> boolean areEqual(final Entry<E>[]... arrays) {
        for (int i = 0; i < arrays.length - 1; ++i) {
            if (arrays[i].length != arrays[i + 1].length) {
                return false;
            }
        }

        for (int i = 0; i < arrays[0].length; ++i) {
            for (int j = 0; j < arrays.length - 1; ++j) {
                if (!Objects.equals(arrays[j][i], arrays[j + 1][i])) {
                    return false;
                }
            }
        }

        return true;
    }
    
    /**
     * Checks whether the range <code>array[fromIndex, toIndex)</code> is 
     * sorted.
     * 
     * @param <E> the type of entries' satellite data.
     * @param array the actual array.
     * @param fromIndex the least index of the range to check.
     * @param toIndex the index pointing to the one past the last index of the
     *        range.
     * 
     * @return <code>true</code> if the range is sorted. <code>false</code> 
     *         otherwise.
     */
    public static final <E extends Comparable<? super E>> 
        boolean isSorted(final E[] array, 
                         final int fromIndex,
                         final int toIndex) {
        for (int i = fromIndex; i < toIndex - 1; ++i) {
            if (array[i].compareTo(array[i + 1]) > 0) {
                return false;
            }
        }

        return true;
    }

    /**
     * Checks whether the entire input array is sorted.
     * 
     * @param <E>   the type of the entries' satellite data.
     * @param array the array to check.
     * 
     * @return      <code>true</code> if the entire array is sorted. 
     *              <code>false</code> otherwise.
     */
    public static final <E extends Comparable<? super E>>
        boolean isSorted(final E[] array) {
        return isSorted(array, 0, array.length);       
    }
        
    /**
     * Sorts the range using merge sort and copies the result into the proper 
     * array, if needed.
     * 
     * @param <E>            the type of satellite data in the entries.
     * @param source         the source array.
     * @param target         the target array.
     * @param recursionDepth the depth of recursion. 0 is top level, 7 is the 
     *                       bottom one.
     * @param fromIndex      the least index of the range to sort.
     * @param toIndex        the exclusive end index of the range to sort.
     */
    private static final <E> 
        void mergesortAndCleanUp(final Entry<E>[] source,
                                 final Entry<E>[] target,
                                 final int recursionDepth,
                                 final int fromIndex, 
                                 final int toIndex) {
        final boolean even = mergesort(source, target, fromIndex, toIndex);
            
        if (even) {
            // source contains the sorted bucket.
            if ((recursionDepth & 1) == 1) {
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
            if ((recursionDepth & 1) == 0) {
                // byteIndex = 5, 3, 1.
                // target is buffer, copy to source.
                System.arraycopy(target, 
                                 fromIndex,
                                 source, 
                                 fromIndex, 
                                 toIndex - fromIndex);
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
                                           final int recursionDepth,
                                           final int fromIndex,
                                           final int toIndex) {
        // Try merge sort.
        if (toIndex - fromIndex <= MERGESORT_THRESHOLD) {
            mergesortAndCleanUp(source, 
                                target, 
                                recursionDepth,
                                fromIndex,
                                toIndex);
            return;
        }
        
        final int[] bucketSizeMap = new int[BUCKETS];
        final int[] startIndexMap = new int[BUCKETS];
        final int[] processedMap  = new int[BUCKETS];
        
        // Compute the size of each bucket.
        for (int i = fromIndex; i < toIndex; ++i) {
            bucketSizeMap[getBucket(source[i].key(), recursionDepth)]++;
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
            final int index = getBucket(source[i].key(), recursionDepth);
            target[startIndexMap[index] + processedMap[index]++] = e;
        }
        
        if (recursionDepth == 7) {
            // There is nowhere to recur, return.
            return;
        }
        
        // Recur to sort each bucket.
        for (int i = 0; i != BUCKETS; ++i) {
            if (bucketSizeMap[i] != 0) {
                sortImpl(target,
                         source,
                         recursionDepth + 1,
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
                    t[i++] = s[right].key() < s[left].key() ?
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
         * The depth of recursion. 0 is top level, 7 is the bottom one.
         */
        private final int recursionDepth;
        
        /**
         * Specifies the least index of the range to process.
         */
        private final int fromIndex;
        
        /**
         * Specifies the index past one element in the range to process.
         */
        private final int toIndex;
        
        BucketSizeCounter(final Entry<E>[] source,
                          final int recursionDepth,
                          final int fromIndex,
                          final int toIndex) {
            this.source = source;
            this.recursionDepth = recursionDepth;
            this.fromIndex = fromIndex;
            this.toIndex = toIndex;
        }
        
        @Override
        public void run() {
            this.localBucketSizeMap = new int[BUCKETS];
            
            for (int i = fromIndex; i < toIndex; ++i) {
                localBucketSizeMap[getBucket(source[i].key(), 
                                   recursionDepth)]++;
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
         * The depth of recursion. 0 is top level, 7 is the bottom one.
         */
        private final int recursionDepth;
        
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
                       final int recursionDepth,
                       final int fromIndex,
                       final int toIndex) {
            this.startIndexMap = startIndexMap;
            this.processedMap = processedMap;
            this.source = source;
            this.target = target;
            this.recursionDepth = recursionDepth;
            this.fromIndex = fromIndex;
            this.toIndex = toIndex;
        }
        
        @Override
        public void run() {
            for (int i = fromIndex; i < toIndex; ++i) {
                final Entry<E> e = source[i];
                final int index = getBucket(e.key(), recursionDepth);
                target[startIndexMap[index] + processedMap[index]++] = e;
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
                                     task.recursionDepth,
                                     task.fromIndex,
                                     task.toIndex);
                } else {
                    sortImpl(task.source,
                             task.target,
                             task.recursionDepth,
                             task.fromIndex,
                             task.toIndex);
                }
            }
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
         * The depth of recursion. 0 is the top level, 7 is the bottom one.
         */
        private final int recursionDepth;
        
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
             final int recursionDepth,
             final int fromIndex,
             final int toIndex) {
            this.source = source;
            this.target = target;
            this.threads = threads;
            this.recursionDepth = recursionDepth;
            this.fromIndex = fromIndex;
            this.toIndex = toIndex;
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
                                                   final int recursionDepth,
                                                   final int fromIndex,
                                                   final int toIndex) {
        final int RANGE_LENGTH = toIndex - fromIndex;
        
        if (RANGE_LENGTH <= MERGESORT_THRESHOLD) {
            mergesortAndCleanUp(source, 
                                target, 
                                recursionDepth,
                                fromIndex,
                                toIndex);
            return;
        }
        
        if (threads < 2) {
            sortImpl(source, target, recursionDepth, fromIndex, toIndex);
            return;
        }
        
        // Create the bucket size counter threads.
        final BucketSizeCounter[] counters = new BucketSizeCounter[threads];
        final int SUB_RANGE_LENGTH = RANGE_LENGTH / threads;
        int start = fromIndex;
        
        for (int i = 0; i != threads - 1; ++i, start += SUB_RANGE_LENGTH) {
            counters[i] = new BucketSizeCounter<>(source,
                                                  recursionDepth,
                                                  start,
                                                  start + SUB_RANGE_LENGTH);
            counters[i].start();
        }
        
        counters[threads - 1] = 
                new BucketSizeCounter<>(source,
                                        recursionDepth,
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
                                         recursionDepth,
                                         startIndex,
                                         startIndex + SUB_RANGE_LENGTH);
            inserters[i].start();
        }
        
        // Run the last inserter in this thread while other are on their ways.
        new BucketInserter<>(startIndexMap,
                             processedMaps[threads - 1],
                             source,
                             target,
                             recursionDepth,
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
        
        if (recursionDepth == 7) {
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
                                  recursionDepth + 1,
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
    
    private static final int getBucket(final long key, 
                                       final int recursionDepth) {
        final int bitShift = 64 - (recursionDepth + 1) * BITS_PER_BUCKET;
        return (int)((key ^ SIGN_MASK) >>> bitShift) & BUCKET_MASK;
    }
}
