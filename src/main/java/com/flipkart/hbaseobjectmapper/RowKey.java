package com.flipkart.hbaseobjectmapper;

import org.apache.hadoop.hbase.util.Bytes;

import java.util.Arrays;

/**
 * Wrapper class allows byte arrays to be used as keys in Java Maps
 */
public class RowKey implements Comparable<RowKey> {
    static Bytes.ByteArrayComparator comparator = new Bytes.ByteArrayComparator();
    private final byte[] key;

    public RowKey(byte[] key) {
        this.key = key;
    }

    public byte[] bytes() {
        return key;
    }

    @Override
    public boolean equals(Object other) {
        if (!(other instanceof RowKey)) {
            return false;
        }
        return Arrays.equals(key, ((RowKey) other).key);
    }

    @Override
    public int hashCode() {
        return Arrays.hashCode(key);
    }

    @Override
    public int compareTo(RowKey other) {
        return comparator.compare(key, other.bytes());
    }
}