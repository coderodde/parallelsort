package net.coderodde.util;

import java.util.Arrays;
import java.util.Random;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.stream.IntStream;
import static org.junit.Assert.assertTrue;
import org.junit.Test;

public class CoderoddeArraysTest {
    
    private static final Logger LOGGER = 
            Logger.getLogger("CoderoddeArraysTest logger");
            
    
    @Test
    public void testInsertionSort() {
        final int repeats = 50;
        final int maxArrayLength = 100;
        final long seed = System.currentTimeMillis();
        final Random random = new Random(seed);
        LOGGER.log(Level.INFO, "Seed = {0}", seed);
        
        IntStream.range(0, repeats).forEach((i) -> {
            final int arrayLength = random.nextInt(maxArrayLength + 1);
            long[] array = random.longs(arrayLength).toArray();
            long[] expected = array.clone();
            int endIndex = random.nextInt(array.length) + 1;
            int startIndex = random.nextInt(endIndex);
            CoderoddeArrays.insertionsort(array,
                                          startIndex, 
                                          endIndex);
            Arrays.sort(expected,
                        startIndex, 
                        endIndex);
            
            assertTrue(Arrays.equals(expected, array));
        });
    }
    
    @Test
    public void testInsertionSortByKeys() {
        final int repeats = 50;
        final int maxArrayLength = 10000;
        final long seed = System.currentTimeMillis();
        final Random random = new Random(seed);
        LOGGER.log(Level.INFO, "Seed = {0}", seed);
        
        IntStream.range(0, repeats).forEach((i) -> {
            final int arrayLength = random.nextInt(maxArrayLength + 1);
            long[] array = random.longs(arrayLength).toArray();
            long[] expected = array.clone();
            int endIndex = random.nextInt(array.length) + 1;
            int startIndex = random.nextInt(endIndex);
            CoderoddeArrays.quicksort(array,
                                      startIndex,
                                      endIndex);
            Arrays.sort(expected,
                        startIndex, 
                        endIndex);
            
            assertTrue(Arrays.equals(expected, array));
        });
        
    }
}
