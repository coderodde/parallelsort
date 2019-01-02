package net.coderodde.util;

import java.util.Arrays;
import java.util.Random;
import java.util.stream.IntStream;
import org.junit.Test;
import static org.junit.Assert.*;
;
import static net.coderodde.util.ParallelMSDRadixsort.insertionsortBucketIndices;
import static net.coderodde.util.ParallelMSDRadixsort.quicksortBucketIndices;

public final class ParallelMSDRadixsortTest {
    
    @Test
    public void testInsertionSortBucketIndices() {
        int[] array = { 0, 1, 3, 5, 6, 7, 9 };
        int[] keys  = { 3, 5, 1, 3, 8, 6, 2, 1, 4, 5 };
        Integer[] expected = { 0, 3, 5, 1, 7, 6, 9 };
        
        arraysSort(expected, 1, 6, keys);
        insertionsortBucketIndices(array, 1, 6, keys);
        
        for (int i = 0; i < expected.length; i++) {
            assertEquals(expected[i], (Integer) array[i]);
        }
        
        final long seed = System.currentTimeMillis();
        final Random random = new Random(seed);
        System.out.println("Seed = " + seed);
        
        for (int i = 0; i < 100; i++) {
            final int length = random.nextInt(96) + 5; // From 5 to 100 elements.
            final int[] actual = IntStream.range(0, length).toArray();
            keys = random.ints(length).toArray();
            final int index1 = random.nextInt(length),
                      index2 = random.nextInt(length),
                      fromIndex,
                      toIndex;
            
            if (index1 < index2) {
                fromIndex = index1;
                toIndex = index2;
            } else {
                fromIndex = index2;
                toIndex = index1;
            }   
            
            insertionsortBucketIndices(actual, 
                                       fromIndex, 
                                       toIndex, 
                                       keys);
            
            expected = toIntegerArray(actual);
            arraysSort(expected, 
                        fromIndex, 
                        toIndex, 
                        keys);
            
            assertTrue(Arrays.equals(expected,
                                     toIntegerArray(actual)));
        }
    }
    
    @Test
    public void testQuicksortBucketIndices() {
        final long seed = System.currentTimeMillis();
        final Random random = new Random(seed);
        System.out.println("Seed = " + seed);
        
        for (int i = 0; i < 10; i++) {
            final int length = random.nextInt(3996) + 5; // From 5 to 4000 elements.
            final int[] actual = IntStream.range(0, length).toArray();
            final int[] keys = random.ints(length, 0, 1000).toArray();
            final int index1 = random.nextInt(length),
                      index2 = random.nextInt(length),
                      fromIndex,
                      toIndex;
            
            if (index1 < index2) {
                fromIndex = index1;
                toIndex = index2;
            } else {
                fromIndex = index2;
                toIndex = index1;
            }
            
            quicksortBucketIndices(actual, 
                                   fromIndex, 
                                   toIndex, 
                                   keys);
            
            final Integer[] expected = toIntegerArray(actual);
            arraysSort(expected, 
                       fromIndex, 
                       toIndex, 
                       keys);
            
            assertTrue(Arrays.equals(expected,
                                     toIntegerArray(actual)));
        }
    }
    
    @Test
    public void testInsertionsortLong() {
        long seed = System.currentTimeMillis();
        Random random = new Random(seed);
        
        for (int i = 0; i < 100; i++) {
            long[] array1 = random.longs(random.nextInt(30) + 1).toArray();
            long[] array2 = array1.clone();
            
            Arrays.sort(array1);
            ParallelMSDRadixsort.insertiont(array2, 0, array2.length);
            
            assertTrue(Arrays.equals(array1, array2));
        }
    }
    
    @Test
    public void testQuicksortLong() {
        long seed = System.currentTimeMillis();
        Random random = new Random(seed);
        
        for (int i = 0; i < 100; i++) {
            long[] array1 = random.longs(random.nextInt(500) + 1).toArray();
            long[] array2 = array1.clone();
            
            Arrays.sort(array1);
            ParallelMSDRadixsort.quicksort(array2, 0, array2.length);
            
            assertTrue(Arrays.equals(array1, array2));
        }
    }
    
    private static Integer[] toIntegerArray(int[] array) {
        Integer[] result = new Integer[array.length];
        
        for (int i = 0; i < result.length; i++) {
            result[i] = array[i];
        }
        
        return result;
    }
    
    private static void arraysSort(Integer[] array,
                                   int fromIndex,
                                   int toIndex, 
                                   int[] keys) {
        Arrays.sort(array, fromIndex, toIndex, (a, b) -> {
            return Integer.compare(keys[a], keys[b]);
        });
    }
}

