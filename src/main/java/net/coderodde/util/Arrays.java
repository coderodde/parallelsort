package net.coderodde.util;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

public class Arrays {

    private static final int BUCKETS = 256;
    private static final int BITS_PER_BYTE = 8;
    private static final int RIGHT_SHIFT_AMOUNT = 56;
    private static final int MOST_SIGNIFICANT_BYTE_INDEX = 7;
    private static final int THREAD_THRESHOLD = 1 << 16;
    private static final int MERGESORT_THRESHOLD = 512;
    private static final int LEAST_SIGNED_BUCKET_INDEX = 128;
    
    /**
     * The wrapper class holding a satellite datum and the key.
     * 
     * @param <E> the type of a satellite datum.
     */
    public static final class Entry<E> implements Comparable<Entry<E>> {
        public final long key;
        public E satelliteData;
        
        public Entry(final long key, final E satelliteData) {
            this.key = key;
            this.satelliteData = satelliteData;
        }

        @Override
        public int compareTo(Entry<E> o) {
            final long delta = key - o.key;
            return delta < 0 ? -1 : (delta > 0 ? 1 : 0);
        }
    }
        
    public static <E> boolean isSorted(final Entry<E>[] array, 
                                       final int fromIndex, 
                                       final int toIndex) {
            for (int i = fromIndex; i < toIndex - 1; ++i) {
                if (array[i].compareTo(array[i + 1]) > 0) {
                    return false;
                }
            }
            
            return true;
    }
        
    public static <E> boolean isSorted(final Entry<E>[] array) {
        return isSorted(array, 0, array.length);
    }
    
    public static <E> void parallelSort(final Entry<E>[] array) {
        parallelSort(array, 0, array.length);
    }
    
    public static <E> void parallelSort(final Entry<E>[] array,
                                        final int fromIndex,
                                        final int toIndex) {
        if (toIndex - fromIndex < 2) {
            return;
        }
        
        final Entry<E>[] buffer = array.clone();
        parallelSortImplTopLevel(array, buffer, fromIndex, toIndex);
    }
    
    private static <E> void parallelSortImplTopLevel(final Entry<E>[] array,
                                                     final Entry<E>[] buffer,
                                                     final int fromIndex,
                                                     final int toIndex) {
        final int RANGE_LENGTH = toIndex - fromIndex;
        
        if (RANGE_LENGTH <= MERGESORT_THRESHOLD) {
            final int PASSES = (int)(Math.ceil(Math.log(RANGE_LENGTH) /
                                               Math.log(2)));
            
            if ((PASSES & 1) == 0) {
                mergesort(array, buffer, fromIndex, toIndex);
            } else {
                mergesort(buffer, array, fromIndex, toIndex);
            }
            
            return;
        }
        
        final int THREADS = 1;/*Math.min(Runtime.getRuntime().availableProcessors(),
                                     RANGE_LENGTH / THREAD_THRESHOLD);*/
        System.out.println("Top threads: " + THREADS);
        
        if (THREADS < 2) {
            System.out.println("Resorting to sequential sort.");
            sortTopImpl(array, buffer, fromIndex, toIndex);
            return;
        }
        
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
        
        for (int i = 0; i != THREADS; ++i) {
            for (int j = 0; j != BUCKETS; ++j) {
                bucketSizeMap[j] += counters[i].localBucketSizeMap[j];
            }
        }
        
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
        
        final BucketInserter<E>[] inserters = new BucketInserter[THREADS - 1];
        final int[][] processedMaps = new int[THREADS][BUCKETS];
        
        for (int i = 1; i != THREADS; ++i) {
            int[] partialBucketSizeMap = counters[i - 1].localBucketSizeMap;
            
            for (int j = 0; j != BUCKETS; ++j) {
                processedMaps[i][j] = 
                        processedMaps[i - 1][j] + partialBucketSizeMap[j];
            }
        }
        
        int startIndex = fromIndex;
        
        for (int i = 0; i != THREADS - 1; ++i, startIndex += SUB_RANGE_LENGTH) {
            inserters[i] =
                    new BucketInserter<>(startIndexMap,
                                         processedMaps[i],
                                         buffer,
                                         array,
                                         MOST_SIGNIFICANT_BYTE_INDEX,
                                         startIndex,
                                         startIndex + SUB_RANGE_LENGTH);
            inserters[i].start();
        }
        
        new BucketInserter<>(startIndexMap,
                             processedMaps[THREADS - 1],
                             buffer,
                             array,
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
        
        final int SPAWN_DEGREE = Math.min(nonEmptyBucketAmount, THREADS);
        final List<Integer>[] bucketIndexListArray = new List[SPAWN_DEGREE];
        
        for (int i = 0; i != SPAWN_DEGREE; ++i) {
            bucketIndexListArray[i] = new ArrayList<>(nonEmptyBucketAmount);
        }
        
        final int[] threadCountMap = new int[SPAWN_DEGREE];
        
        // 8 / 3 = 2
        for (int i = 0; i != SPAWN_DEGREE; ++i) {
            threadCountMap[i] = THREADS / SPAWN_DEGREE;
        }
        
        for (int i = 0; i != THREADS % SPAWN_DEGREE; ++i) {
            ++threadCountMap[i];
        }
    }
    
    private static final <E> void sortTopImpl(final Entry<E>[] array,
                                              final Entry<E>[] buffer,
                                              final int fromIndex,
                                              final int toIndex) {
        final int[] bucketSizeMap = new int[BUCKETS];
        final int[] startIndexMap = new int[BUCKETS];
        final int[] processedMap  = new int[BUCKETS];
        
        for (final Entry<E> e : array) {
            bucketSizeMap[(int)(e.key >>> RIGHT_SHIFT_AMOUNT)]++;
        }
        
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
        
        for (final Entry<E> e : array) {
            final int index = (int)(e.key >>> RIGHT_SHIFT_AMOUNT);
            buffer[startIndexMap[index] + processedMap[index]++] = e;
        }
        
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
    
    private static final <E> void sortImpl(final Entry<E>[] source,
                                           final Entry<E>[] target,
                                           final int byteIndex,
                                           final int fromIndex,
                                           final int toIndex) {
        if (toIndex - fromIndex <= MERGESORT_THRESHOLD) {
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
            
//            // byteIndex is at most 6 here, in which case target is the original 
//            // array.
//            if ((byteIndex & 1) == 1) {
//                // byteIndex = 5, 3, 1.
//                // target is buffer,
//                // source is original array.
//                if (even) {
//                    System.arraycopy(source, 
//                                     fromIndex, 
//                                     target, 
//                                     fromIndex, 
//                                     toIndex - fromIndex);
//                }
//            } else {
//                // byteIndex = 6, 4, 2, 0.
//                // target is original array,
//                // source is buffer.
//                if (!even) {
//                    System.arraycopy(target,
//                                     fromIndex, 
//                                     source, 
//                                     fromIndex, 
//                                     toIndex - fromIndex);
//                }
//            }
            
            return;
        }
        
        final int[] bucketSizeMap = new int[BUCKETS];
        final int[] startIndexMap = new int[BUCKETS];
        final int[] processedMap  = new int[BUCKETS];
        final int LEFT_SHIFT_AMOUNT =
                BITS_PER_BYTE * (MOST_SIGNIFICANT_BYTE_INDEX - byteIndex);
        
        for (int i = fromIndex; i < toIndex; ++i) {
            bucketSizeMap[(int)((source[i].key << LEFT_SHIFT_AMOUNT)
                                             >>> RIGHT_SHIFT_AMOUNT)]++;
        }
        
        startIndexMap[0] = fromIndex;
        
        for (int i = 1; i != BUCKETS; ++i) {
            startIndexMap[i] = startIndexMap[i - 1] +
                               bucketSizeMap[i - 1];
        }
        
        for (int i = fromIndex; i < toIndex; ++i) {
            final Entry<E> e = source[i];
            final int index = (int)((e.key << LEFT_SHIFT_AMOUNT)
                                         >>> RIGHT_SHIFT_AMOUNT);
            target[startIndexMap[index] + processedMap[index]++] = e;
        }
        
        if (byteIndex == 0) {
            // We are done with this bucket.
            return;
        }
        
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
        final int PASSES = (int)(Math.ceil(Math.log(RANGE_LENGTH) / 
                                           Math.log(2)));
        
        Entry<E>[] s = source;
        Entry<E>[] t = target;
        
        for (int width = 1; width < RANGE_LENGTH; width <<= 1) {
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
        
        return (PASSES & 1) == 0;
    }
    
    public static void main(final String... args) {
        final long SEED = System.currentTimeMillis();
        final int SIZE = 10000000;
        final Random rnd = new Random(SEED);
        final Entry<Object>[] array1 = createRandomArray(SIZE, rnd);
        
        System.out.println("Seed: " + SEED);
        
        long ta = System.currentTimeMillis();
        parallelSort(array1);
        long tb = System.currentTimeMillis();
        
        System.out.println("Time: " + (tb - ta) + " ms.");
        System.out.println("Is sorted: " + isSorted(array1));
    }
    
    public static final Entry<Object>[] createRandomArray(final int size,
                                                          final Random rnd) {
        final Entry[] array = new Entry[size];
        
        for (int i = 0; i < size; ++i) {
            final long key = (((long) rnd.nextInt()) << 32) | rnd.nextInt();
            array[i] = new Entry<>(key, null);
        }
        
        return array;
    }
    
    private static final class BucketSizeCounter<E> extends Thread {
        int[] localBucketSizeMap;
        private final Entry<E>[] source;
        private final int byteIndex;
        private final int fromIndex;
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
    
    private static final class BucketInserter<E> extends Thread {
        private final int[] startIndexMap;
        private final int[] processedMap;
        private final Entry<E>[] source;
        private final Entry<E>[] target;
        private final int byteIndex;
        private final int fromIndex;
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
}
