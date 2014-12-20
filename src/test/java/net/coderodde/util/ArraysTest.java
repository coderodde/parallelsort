package net.coderodde.util;

import java.util.Random;
import static net.coderodde.util.Utilities.isSorted;
import org.junit.Test;
import static org.junit.Assert.*;

public class ArraysTest {
    
    private Entry<Object>[] array;
    private final Random rnd = new Random();
    
    @Test
    public void testSort() {
        final int SIZE = 1000000;
        array = new Entry[SIZE];
        
        // Build an array with long keys in which only the least-significant
        // byte has some bits set on.
        for (int i = 0; i < SIZE; ++i) {
            array[i] = new Entry<>(rnd.nextLong() & 0xffL, null);
        }
        
        long ta = System.currentTimeMillis();
        net.coderodde.util.Arrays.parallelSort(array);
        long tb = System.currentTimeMillis();
        
        System.out.println("Sorted in " + (tb - ta) + " ms.");
        assertTrue(isSorted(array));
        
        array = new Entry[SIZE];
        
        for (int i = 0; i < SIZE; ++i) {
            array[i] = new Entry<>(rnd.nextLong() & 0xff00000000000000L, null);
        }
        
        ta = System.currentTimeMillis();
        net.coderodde.util.Arrays.parallelSort(array, 1, array.length - 1);
        tb = System.currentTimeMillis();
        
        System.out.println("Sorted in " + (tb - ta) + " ms.");
        assertTrue(isSorted(array, 1, array.length - 1));
        assertFalse(isSorted(array));
        assertFalse(isSorted(array, 1, array.length));
        assertFalse(isSorted(array, 0, array.length - 1));
        
        array = new Entry[SIZE];
        
        for (int i = 0; i < SIZE; ++i) {
            array[i] = new Entry<>(rnd.nextLong() & 0xff000000000000L, null);
        }
        
        ta = System.currentTimeMillis();
        net.coderodde.util.Arrays.parallelSort(array);
        tb = System.currentTimeMillis();
        
        System.out.println("Sorted in " + (tb - ta) + " ms.");
        assertTrue(isSorted(array));
    }
}
