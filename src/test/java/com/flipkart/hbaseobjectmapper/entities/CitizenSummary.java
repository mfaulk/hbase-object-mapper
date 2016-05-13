package com.flipkart.hbaseobjectmapper.entities;


import com.flipkart.hbaseobjectmapper.HBColumn;
import com.flipkart.hbaseobjectmapper.HBRecord;
import com.flipkart.hbaseobjectmapper.HBRowKey;
import com.flipkart.hbaseobjectmapper.HBTable;
import lombok.ToString;

@ToString
@HBTable("citizen_summary")
public class CitizenSummary implements HBRecord {

    @HBRowKey
    private byte[] key;

    @HBColumn(family = "a", column = "average_age")
    private Float averageAge;

    public CitizenSummary() {
        key = "summary".getBytes();
    }

    @Override
    public byte[] composeRowKey() {
        return key;
    }

    @Override
    public void parseRowKey(byte[] rowKey) {
        key = rowKey;
    }

    public Float getAverageAge() {
        return averageAge;
    }

    public void setAverageAge(float averageAge) {
        this.averageAge = averageAge;
    }
}
