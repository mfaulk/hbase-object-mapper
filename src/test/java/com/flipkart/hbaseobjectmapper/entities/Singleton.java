package com.flipkart.hbaseobjectmapper.entities;

import com.flipkart.hbaseobjectmapper.HBColumn;
import com.flipkart.hbaseobjectmapper.HBRecord;
import com.flipkart.hbaseobjectmapper.HBRowKey;

public class Singleton implements HBRecord {
    private static Singleton ourInstance = new Singleton();

    @HBRowKey
    protected byte[] key = "key".getBytes();

    @HBColumn(family = "f", column = "c")
    String column;

    public static Singleton getInstance() {
        return ourInstance;
    }

    private Singleton() {
        column = "something";
    }

    @Override
    public byte[] composeRowKey() {
        return key;
    }

    @Override
    public void parseRowKey(byte[] rowKey) {
        this.key = rowKey;
    }

}
