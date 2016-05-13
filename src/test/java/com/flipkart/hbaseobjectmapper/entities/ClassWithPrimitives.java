package com.flipkart.hbaseobjectmapper.entities;

import com.flipkart.hbaseobjectmapper.HBColumn;
import com.flipkart.hbaseobjectmapper.HBRecord;
import com.flipkart.hbaseobjectmapper.HBRowKey;

public class ClassWithPrimitives implements HBRecord {
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
    private float f;

    public ClassWithPrimitives() {

    }

    public ClassWithPrimitives(float f) {
        this.f = f;
    }
}
