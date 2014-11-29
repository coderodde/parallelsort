package net.coderodde.util;

import java.util.Random;

public class Utilities {
       
    /**
     * Returns <code>true</code> if the contiguous subsequence 
     * <code>array[fromIndex, toIndex)</code> is sorted, and <code>false</code>
     * otherwise.
     * 
     * @param <E> the type of the satellite data of each entry.
     * @param array the array containing the subsequence.
     * @param fromIndex the index of the leftmost component in the sequence.
     * @param toIndex the index past the rightmost component in the sequence, 
     * i.e., exclusive index.
     * 
     * @return <code>true</code> if the (sub)sequence is sorted, and 
     * <code>false</code> otherwise.
     */
    public static <E> boolean isSorted(final Entry<E>[] array, 
                                       int fromIndex, 
                                       int toIndex) {
        if (fromIndex < 0) {
            fromIndex = 0;
        }
        
        if (toIndex > array.length) {
            toIndex = array.length;
        }
        
        int gotchas = 0;
        
        for (int i = fromIndex; i < toIndex - 1; ++i) {
            if (array[i].compareTo(array[i + 1]) > 0) {
                return false;
            }
        }

        return true;
    }
        
    /**
     * Returns <code>true</code> if the entire array is sorted, and
     * <code>false</code> otherwise.
     * 
     * @param <E> the type of the entries' satellite data.
     * @param array the array to check.
     * 
     * @return <code>true</code> if the array is sorted, and <code>false</code>
     * otherwise.
     */
    public static <E> boolean isSorted(final Entry<E>[] array) {
        return isSorted(array, 0, array.length);
    }
    
    /**
     * Generates an array of entries with random keys and no satellite data.
     * 
     * @param size the length of the requested array.
     * @param rnd the random-number-generator.
     * 
     * @return a random entry array.
     */
    public static final Entry<Object>[] createRandomArray(final int size,
                                                          final Random rnd) {
        final Entry[] array = new Entry[size];
        
        for (int i = 0; i < size; ++i) {
            final long key = rnd.nextLong();
            array[i] = new Entry<>(key, null);
        }
        
        return array;
    }
    
    /**
     * Checks that all the input arrays are of equal length, and contain 
     * exactly the same entry references at corresponding components.
     * 
     * @param <E> the type of entries' satellite data.
     * @param arrays the arrays to check.
     * 
     * @return <code>true</code> if all the arrays are identical, and
     * <code>false</code> otherwise.
     */
    public static final <E> boolean areEqual(final Entry<E>[]... arrays) {
        for (int i = 0; i < arrays.length - 1; ++i) {
            if (arrays[i].length != arrays[i + 1].length) {
                return false;
            }
        }
        
        for (int i = 0; i < arrays[0].length; ++i) {
            for (int j = 0; j < arrays.length - 1; ++j) {
                if (arrays[j][i] != arrays[j + 1][i]) {
                    return false;
                }
            }
        }
        
        return true;
    }
}
