package com.flipkart.hbaseobjectmapper.entities;

import com.flipkart.hbaseobjectmapper.HBColumn;
import com.flipkart.hbaseobjectmapper.HBRecord;
import com.flipkart.hbaseobjectmapper.HBRowKey;

public class ClassWithNoEmptyConstructor implements HBRecord {
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

    @HBColumn(family = "a", column = "b")
    private Integer i;

    public ClassWithNoEmptyConstructor(int i) {
        this.i = i;
    }
}
