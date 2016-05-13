package com.flipkart.hbaseobjectmapper.entities;

import com.flipkart.hbaseobjectmapper.HBRecord;
import com.flipkart.hbaseobjectmapper.HBRowKey;

public class ClassWithNoHBColumns implements HBRecord {
    @HBRowKey
    protected byte[] key = "key".getBytes();

    @Override
    public byte[] composeRowKey() {
        return key;
    }

    @Override
    public void parseRowKey(byte[] rowKey) {
        this.key = rowKey;
    }

    private Float f; //not adding @HBColumn here!
}
