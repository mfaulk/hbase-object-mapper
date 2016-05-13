package com.flipkart.hbaseobjectmapper.entities;

import com.flipkart.hbaseobjectmapper.HBColumnMultiVersion;
import com.flipkart.hbaseobjectmapper.HBRecord;
import com.flipkart.hbaseobjectmapper.HBRowKey;

import java.util.Map;
import java.util.NavigableMap;

public class ClassesWithFieldIncomptibleWithHBColumnMultiVersion {
    public static class NotMap implements HBRecord {
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

        @HBColumnMultiVersion(family = "f", column = "c")
        private Integer i;
    }

    public static class NotNavigableMap implements HBRecord {
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

        @HBColumnMultiVersion(family = "f", column = "c")
        private Map<Long, Integer> i;
    }

    public static class EntryKeyNotLong implements HBRecord {
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

        @HBColumnMultiVersion(family = "f", column = "c")
        private NavigableMap<Integer, Integer> i;
    }
}
