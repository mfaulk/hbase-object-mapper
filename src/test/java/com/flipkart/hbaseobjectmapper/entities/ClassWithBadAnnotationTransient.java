package com.flipkart.hbaseobjectmapper.entities;

import com.flipkart.hbaseobjectmapper.HBColumn;
import com.flipkart.hbaseobjectmapper.HBRecord;
import com.flipkart.hbaseobjectmapper.HBRowKey;

public class ClassWithBadAnnotationTransient implements HBRecord {
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

    @HBColumn(family = "a", column = "first_name")
    private String firstName;
    @HBColumn(family = "a", column = "last_name")
    private String lastName;
    @HBColumn(family = "a", column = "full_name")
    private transient String fullName; // not allowed

    public ClassWithBadAnnotationTransient() {
    }

    public ClassWithBadAnnotationTransient(String firstName, String lastName) {
        this.firstName = firstName;
        this.lastName = lastName;
        this.fullName = firstName + " " + lastName;
    }
}
