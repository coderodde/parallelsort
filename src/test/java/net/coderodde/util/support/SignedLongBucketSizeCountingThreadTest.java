package net.coderodde.util.support;

import java.util.Arrays;
import java.util.Random;
import net.coderodde.util.ParallelMSDRadixsort;
import org.junit.Test;
import static org.junit.Assert.*;

/**
 *
 * @author Rodion "rodde" Efremov
 * @version 1.6 (Mar 4, 2019)
 */
public class SignedLongBucketSizeCountingThreadTest {
    
    private static final long MASK = 0xff00_0000_0000_0000L;
    private static final int MINIMUM_ARRAY_LENGTH = 0;
    private static final int MAXIMUM_ARRAY_LENGTH = 1024;
    
    private final Random random;
    
    public SignedLongBucketSizeCountingThreadTest() {
        long seed = System.nanoTime();
        random = new Random(seed);
        System.out.println("seed = " + seed);
    }
    
    @Test
    public void test() {
        int size = randomBetween(MINIMUM_ARRAY_LENGTH, 
                                 MAXIMUM_ARRAY_LENGTH);
        int fromIndex = random.nextInt(size);
        int toIndex = randomBetween(fromIndex, size);
        
        long[] array = createRandomLongArray(size);
        
        SignedLongBucketSizeCountingThread thread = 
                new SignedLongBucketSizeCountingThread(
                        array, 
                        fromIndex, 
                        toIndex);
        
        assertTrue(
                Arrays.equals(
                        count(array, 
                              fromIndex,
                              toIndex), 
                        thread.localBucketSizeMap));
    }
    
    private static final int[] count(long[] array, int fromIndex, int toIndex) {
        int[] bucketSizeArray = new int[ParallelMSDRadixsort.BUCKETS];
        
        for (int i = fromIndex; i < toIndex; i++) {
            long key = array[i];
            int bucketIndex = (int)(key >>> 7 * Byte.SIZE) & 0xff;
            bucketSizeArray[bucketIndex]++;
        }
        
        return bucketSizeArray;
    }
    
    private long[] createRandomLongArray(int size) {
        long[] array = new long[size];
        
        for (int i = 0; i < size; i++) {
            array[i] = random.nextLong() & MASK;
        }
        
        return array;
    }
    
    private int randomBetween(int a, int b) {
        return random.nextInt(b - a + 1) + a;
    }
}
