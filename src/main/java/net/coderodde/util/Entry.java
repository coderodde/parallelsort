package net.coderodde.util;

/**
 * The wrapper class holding a satellite datum and the key.
 *
 * @param <E> the type of a satellite datum.
 */
public final class Entry<E> implements Comparable<Entry<E>> {

    /**
     * The sorting key.
     */
    private final long key;

    /**
     * The satellite data.
     */
    private final E satelliteData;

    /**
     * Constructs a new <code>Entry</code> with key <code>key</code> and 
     * the satellite datum <code>satelliteData</code>.
     * 
     * @param key the key of this entry.
     * @param satelliteData the satellite data associated with the key.
     */
    public Entry(final long key, final E satelliteData) {
        this.key = key;
        this.satelliteData = satelliteData;
    }

    public long key() {
        return key;
    }

    public E satelliteData() {
        return satelliteData;
    }

    /**
     * Compares this <code>Entry</code> with another.
     * 
     * @param o the entry to compare against.
     * 
     * @return a negative value if this entry's key is less than that of 
     * <code>o</code>, a positive value if this entry's key is greater than that
     * of <code>o</code>, or 0 if the two keys are equal.
     */
    @Override
    public int compareTo(Entry<E> o) {
        return Long.compare(key, o.key);
    }
}
