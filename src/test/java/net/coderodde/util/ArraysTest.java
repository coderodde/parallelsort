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
        
        array[0].key = Long.MAX_VALUE;
        array[array.length - 1].key = Long.MIN_VALUE;
        
        ta = System.currentTimeMillis();
        net.coderodde.util.Arrays.parallelSort(array, 1, array.length - 1);
        tb = System.currentTimeMillis();
        
        System.out.println("Sorted in " + (tb - ta) + " ms.");
        assertTrue(isSorted(array, 1, array.length - 1));
        assertFalse(isSorted(array));
        assertFalse(isSorted(array, 1, array.length));
        assertFalse(isSorted(array, 0, array.length - 1));
    }
}
