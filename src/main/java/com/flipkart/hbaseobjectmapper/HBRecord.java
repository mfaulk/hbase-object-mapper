package com.flipkart.hbaseobjectmapper;

import org.apache.hadoop.hbase.util.Bytes;

/**
 * Entities that need to be mapped to HBase table need to implement this interface
 */
public interface HBRecord {

    /**
     * Forms the row key required for HBase from class variables
     *
     * @return Row key
     */
    public byte[] composeRowKey();

    public void parseRowKey(byte[] bytes);

}
