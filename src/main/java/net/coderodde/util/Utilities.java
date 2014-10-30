package net.coderodde.util;

import java.util.Random;

public class Utilities {
       
    public static <E> boolean isSorted(final Entry<E>[] array, 
                                       final int fromIndex, 
                                       final int toIndex) {
            for (int i = fromIndex; i < toIndex - 1; ++i) {
                if (array[i].compareTo(array[i + 1]) > 0) {
                    return false;
                }
            }
            
            return true;
    }
        
    public static <E> boolean isSorted(final Entry<E>[] array) {
        return isSorted(array, 0, array.length);
    }
    
    
    public static final Entry<Object>[] createRandomArray(final int size,
                                                          final Random rnd) {
        final Entry[] array = new Entry[size];
        
        for (int i = 0; i < size; ++i) {
            final long key = (((long) rnd.nextInt()) << 32) | rnd.nextInt();
            array[i] = new Entry<>(key, null);
        }
        
        return array;
    }
    
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
