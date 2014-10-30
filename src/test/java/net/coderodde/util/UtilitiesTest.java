package net.coderodde.util;

import java.util.Random;
import static net.coderodde.util.Utilities.createRandomArray;
import static net.coderodde.util.Utilities.isSorted;
import org.junit.Test;
import static org.junit.Assert.*;

public class UtilitiesTest {
    
    @Test
    public void testIsSorted() {
        Entry<Object>[] array = new Entry[0];
        assertTrue(isSorted(array));
        
        array = new Entry[1];
        assertTrue(isSorted(array));
        
        array = new Entry[]{ new Entry<>(10L, null), 
                             new Entry<>(-9L, null) };
        
        assertFalse(isSorted(array));
        assertFalse(isSorted(array, -1, array.length));
        assertFalse(isSorted(array, 0, array.length + 1));
        assertFalse(isSorted(array, -1, array.length + 1));
        
        // Sort array.
        Entry<Object> tmp = array[0];
        array[0] = array[1];
        array[1] = tmp;
        
        assertTrue(isSorted(array));
        assertTrue(isSorted(array, -1, array.length));
        assertTrue(isSorted(array, 0, array.length + 1));
        assertTrue(isSorted(array, -1, array.length + 1));
        
        array = new Entry[]{ new Entry<>(2L, null), 
                             new Entry<>(1L, null),
                             new Entry<>(3L, null)};
        
        assertFalse(isSorted(array));
        assertFalse(isSorted(array, -1, 2));
        assertTrue(isSorted(array, 1, 5));
        assertTrue(isSorted(array, 1, 2));
        assertTrue(isSorted(array, 1, 3));
    }
    
    @Test
    public void testCreateRandomArray() {
        final int SIZE = 100;
        final Entry<Object>[] array = createRandomArray(SIZE, new Random());
        
        assertTrue(array.length == SIZE);
        
        for (final Entry<Object> e : array) {
            assertNotNull(e);
            assertNull(e.satelliteData);
        }
    }
}
