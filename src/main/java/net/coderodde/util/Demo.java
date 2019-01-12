package net.coderodde.util;

import java.util.Arrays;
import java.util.Random;
import static net.coderodde.util.Utilities.areEqual;
import static net.coderodde.util.Utilities.isSorted;
import static net.coderodde.util.Utilities.createRandomArray;

public class Demo {

    public static void main(String[] args) {
        benchmarkLongArrays();
    }
    
    public static void benchmarkLongArrays() {
        final long SEED = System.currentTimeMillis();
        final int SIZE = 10_000_000;
        final Random rnd = new Random(SEED);
        final long[] array1 = rnd.longs(SIZE).toArray();
        final long[] array2 = array1.clone();
        
        System.out.println("> benchmarkLongArrays(), seed = " + SEED);
        
        long startTime = System.currentTimeMillis();
        ParallelMSDRadixsort.parallelSort(array1, 10, array1.length - 10);
        long endTime = System.currentTimeMillis();
        
        System.out.println("> ParallelMSDRadixsort.parallelSort in " +
                           (endTime - startTime) + " ms.");
        
        startTime = System.currentTimeMillis();
        Arrays.sort(array2, 10, array2.length - 10);
        endTime = System.currentTimeMillis();
        
        System.out.println("> Arrays.parallelSort in " +
                           (endTime - startTime) + " ms.");
        System.out.println("Algorithms agree: " + areEqual(array1, array2));
    }
    
//    public static void benchmarkParallelArrays() {
//        final long SEED = System.currentTimeMillis();
//        final int SIZE = 60_000_000;
//        final Random rnd = new Random(SEED);
//        final Entry<Object>[] array1 = createRandomArray(SIZE, rnd);
//        final Entry<Object>[] array2 = array1.clone();
//        final Entry<Object>[] array3 = array1.clone();
//        
//        System.out.println("Seed: " + SEED);
//        
//        long ta = System.currentTimeMillis();
//        parallelSort(array1);
//        long tb = System.currentTimeMillis();
//        
//        System.out.println("My parallel sort; time:    " + (tb - ta) + " ms.");
//        System.out.println("Is sorted: " + isSorted(array1));
//        
//        ta = System.currentTimeMillis();
//        java.util.Arrays.parallelSort(array2);
//        tb = System.currentTimeMillis();
//        
//        System.out.println("JDK parallel sort; time:   " + (tb - ta) + " ms.");
//        System.out.println("Is sorted: " + isSorted(array2));
//        
//        ta = System.currentTimeMillis();
//        java.util.Arrays.sort(array3);
//        tb = System.currentTimeMillis();
//        
//        System.out.println("JDK sequential sort; time: " + (tb - ta) + " ms.");
//        System.out.println("Is sorted: " + isSorted(array3));
//        
//        System.out.println("All arrays equal: " + areEqual(array1, 
//                                                           array2, 
//                                                           array3));
//    }
    
    private static boolean areEqual(long[] array1, long[] array2) {
        if (array1.length != array2.length) {
            return false;
        }
        
        for (int i = 0; i < array1.length; i++) {
            if (array1[i] != array2[i]) {
                return false;
            }
        }
        
        return true;
    }
}
