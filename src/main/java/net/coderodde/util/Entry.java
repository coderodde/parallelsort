package net.coderodde.util;

/**
 * The wrapper class holding a satellite datum and the key.
 *
 * @param <E> the type of a satellite datum.
 */
public final class Entry<E> implements Comparable<Entry<E>> {
    public long key;
    public E satelliteData;

    public Entry(final long key, final E satelliteData) {
        this.key = key;
        this.satelliteData = satelliteData;
    }

    @Override
    public int compareTo(Entry<E> o) {
        if (key < o.key) {
            return -1;
        } else if (key > o.key) {
            return 1;
        } else {
            return 0;
        }
    }
}
