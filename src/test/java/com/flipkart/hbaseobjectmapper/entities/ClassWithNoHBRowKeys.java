package com.flipkart.hbaseobjectmapper.entities;

import com.flipkart.hbaseobjectmapper.HBColumn;
import com.flipkart.hbaseobjectmapper.HBRecord;
import com.flipkart.hbaseobjectmapper.HBRowKey;

public class ClassWithNoHBRowKeys implements HBRecord {
    // No HBRowKey Annotation.
    protected byte[] key = "key".getBytes();

    @Override
    public byte[] composeRowKey() {
        return key;
    }

    @Override
    public void parseRowKey(byte[] rowKey) {
        this.key = rowKey;
    }
    @HBColumn(family = "f", column = "c")
    private Float f;
}
