package net.coderodde.util;

import java.util.Arrays;
import java.util.Random;
import java.util.stream.IntStream;
import org.junit.Test;
import static org.junit.Assert.*;
import static net.coderodde.util.ParallelMSDRadixsort.insertionsortBucketIndices;

public final class ParallelMSDRadixsortTest {
    
    @Test
    public void testInsertionSortBucketIndices() {
        int[] array = { 0, 1, 3, 5, 6, 7, 9 };
        int[] keys  = { 3, 5, 1, 3, 8, 6, 2, 1, 4, 5 };
        Integer[] expected = { 0, 3, 5, 1, 7, 6, 9 };
        
        integerSort(expected, 1, 6, keys);
        
        insertionsortBucketIndices(array, 1, 6, keys);
        System.out.println(Arrays.toString(expected));
        System.out.println(Arrays.toString(array));
        
        for (int i = 0; i < expected.length; i++) {
            assertEquals(expected[i], (Integer) array[i]);
        }
        
        final long seed = System.currentTimeMillis();
        final Random random = new Random(seed);
        System.out.println("Seed = " + seed);
        
        for (int i = 0; i < 100; i++) {
            final int length = random.nextInt(100) + 1;
            final int[] actual = IntStream.range(0, length).toArray();
            keys = random.ints(length).toArray();
            final int index1 = random.nextInt(length);
            final int index2 = random.nextInt(length);
            
            final int fromIndex;
            final int toIndex;
            
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
            integerSort(expected, 
                        fromIndex, 
                        toIndex, 
                        keys);
            
            assertTrue(Arrays.equals(expected,
                                     toIntegerArray(actual)));
        }
    }
    
    private static Integer[] toIntegerArray(int[] array) {
        Integer[] result = new Integer[array.length];
        
        for (int i = 0; i < result.length; i++) {
            result[i] = array[i];
        }
        
        return result;
    }
    
    private static void integerSort(Integer[] array,
                                    int fromIndex,
                                    int toIndex, 
                                    int[] keys) {
        Arrays.sort(array, fromIndex, toIndex, (a, b) -> {
            return Integer.compare(keys[a], keys[b]);
        });
    }
}

