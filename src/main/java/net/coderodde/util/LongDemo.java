package net.coderodde.util;

import java.util.Arrays;
import java.util.Random;

/**
 *
 * @author Rodion "rodde" Efremov
 * @version 1.6 (Dec 3, 2018)
 */
public final class LongDemo {
    
    private static final int LONGS = 6_000_000;
    private static final int FROM_INDEX = 10;
    private static final int TO_INDEX = LONGS - 10;
    
    public static void majin(String[] args) {
        long seed = System.currentTimeMillis();
        Random random = new Random(seed);
        long[] array1 = getRandomLongArray(LONGS, random);
        long[] array2 = Arrays.copyOf(array1, array1.length);
        System.out.println("Seed = " + seed);
        
        long startTime = System.currentTimeMillis();
        
        ParallelMSDRadixsort.sort(array1, FROM_INDEX, TO_INDEX);
        
        long endTime = System.currentTimeMillis();
        
        System.out.println("My sort in " + (endTime - startTime) + " ms.");
        
        startTime = System.currentTimeMillis();
        Arrays.sort(array2, FROM_INDEX, TO_INDEX);
        endTime = System.currentTimeMillis();
        
        System.out.println("Arrays.sort in " + (endTime - startTime) + " ms.");
        System.out.println(
                "Algorithms agree: " + Arrays.equals(array1, array2));
    }
    
    private static long[] getRandomLongArray(int length, Random random) {
        return random.longs(length).toArray();
    }
}
