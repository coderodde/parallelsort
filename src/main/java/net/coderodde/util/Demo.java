package net.coderodde.util;

import java.util.Random;
import static net.coderodde.util.CoderoddeArrays.parallelSort;
import static net.coderodde.util.Utilities.areEqual;
import static net.coderodde.util.Utilities.isSorted;
import static net.coderodde.util.Utilities.createRandomArray;

public class Demo {

    public static void main(String[] args) {
        benchmarkLongArrays();
    }
    
    public static void benchmarkLongArrays() {
        final long SEED = System.currentTimeMillis();
        final int SIZE = 60_000_000;
        final Random rnd = new Random(SEED);
        final long[] array1 = createRandomLongArray(SIZE, rnd);
        final long[] array2 = array1.clone();
        final long[] array3 = array1.clone();
        
        long startTime = System.currentTimeMillis();
        
        parallelSort(array1);
        
        long endTime = System.currentTimeMillis();
        
        
    }
    
    public static void benchmarkParallelArrays() {
        final long SEED = System.currentTimeMillis();
        final int SIZE = 60_000_000;
        final Random rnd = new Random(SEED);
        final Entry<Object>[] array1 = createRandomArray(SIZE, rnd);
        final Entry<Object>[] array2 = array1.clone();
        final Entry<Object>[] array3 = array1.clone();
        
        System.out.println("Seed: " + SEED);
        
        long ta = System.currentTimeMillis();
        parallelSort(array1);
        long tb = System.currentTimeMillis();
        
        System.out.println("My parallel sort; time:    " + (tb - ta) + " ms.");
        System.out.println("Is sorted: " + isSorted(array1));
        
        ta = System.currentTimeMillis();
        java.util.Arrays.parallelSort(array2);
        tb = System.currentTimeMillis();
        
        System.out.println("JDK parallel sort; time:   " + (tb - ta) + " ms.");
        System.out.println("Is sorted: " + isSorted(array2));
        
        ta = System.currentTimeMillis();
        java.util.Arrays.sort(array3);
        tb = System.currentTimeMillis();
        
        System.out.println("JDK sequential sort; time: " + (tb - ta) + " ms.");
        System.out.println("Is sorted: " + isSorted(array3));
        
        System.out.println("All arrays equal: " + areEqual(array1, 
                                                           array2, 
                                                           array3));
    }

    private static long[] createRandomLongArray(int size, Random rnd) {
        long[] array = new long[size];
        
        for (int i = 0; i != size; i++) {
            array[i] = rnd.nextLong();
        }
        
        return array;
    }
}
