package net.coderodde.util;

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
        
        final int THREADS = 16;/*Math.min(Runtime.getRuntime().availableProcessors(),
                                     RANGE_LENGTH / THREAD_THRESHOLD);*/
        
//        if (THREADS < 2) {
//            System.out.println("Resorting to sequential sort.");
//            
//            return;
//        }
        
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
        
        
    }
    
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
}
