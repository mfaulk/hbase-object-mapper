package com.flipkart.hbaseobjectmapper.entities;

import com.flipkart.hbaseobjectmapper.*;
import com.flipkart.hbaseobjectmapper.exceptions.HBRowKeyFieldCantBeNullException;
import com.flipkart.hbaseobjectmapper.exceptions.RowKeyCouldNotBeParsedException;
import lombok.EqualsAndHashCode;
import lombok.ToString;
import org.apache.commons.lang.ArrayUtils;
import org.apache.hadoop.hbase.util.Bytes;

import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.util.*;

@ToString
@EqualsAndHashCode
@HBTable("citizens")
public class Citizen implements HBRecord {
    private static final String KEY_DELIM = "#";
    @HBRowKey
    private String countryCode;
    @HBRowKey
    private Integer uid;
    @HBColumn(family = "main", column = "name")
    private String name;
    private transient String nameInUpper;
    @HBColumn(family = "optional", column = "age")
    private Short age;
    @HBColumn(family = "optional", column = "salary")
    private Integer sal;
    @HBColumn(family = "optional", column = "iph")
    private Boolean isPassportHolder;
    @HBColumn(family = "optional", column = "f1")
    private Float f1;
    @HBColumn(family = "optional", column = "f2")
    private Double f2;
    @HBColumn(family = "optional", column = "f3")
    private Long f3;
    @HBColumn(family = "optional", column = "f4")
    private BigDecimal f4;
    @HBColumn(family = "optional", column = "pincode", serializeAsString = true)
    private Integer pincode;
    @HBColumnMultiVersion(family = "optional", column = "phone_number")
    private NavigableMap<Long, Integer> phoneNumberHistory;
    @HBColumn(family = "optional", column = "flags")
    private Map<String, Integer> extraFlags;
    @HBColumn(family = "optional", column = "dependents")
    private Dependents dependents;

    public static byte[] intToByteArray(int value) {
        ByteBuffer b = ByteBuffer.allocate(4);
        //b.order(ByteOrder.BIG_ENDIAN); // optional, the initial order of a byte buffer is always BIG_ENDIAN.
        b.putInt(value);
        return b.array();
    }

    public static List<byte[]> splitTokens(byte[] array, byte[] delimiter) {
        List<byte[]> byteArrays = new LinkedList<>();
        if (delimiter.length == 0) {
            return byteArrays;
        }
        int begin = 0;

        outer:
        for (int i = 0; i < array.length - delimiter.length + 1; i++) {
            for (int j = 0; j < delimiter.length; j++) {
                if (array[i + j] != delimiter[j]) {
                    continue outer;
                }
            }
            byteArrays.add(Arrays.copyOfRange(array, begin, i));
            begin = i + delimiter.length;
        }
        byteArrays.add(Arrays.copyOfRange(array, begin, array.length));
        return byteArrays;
    }


    public Citizen() {
    }

    public Citizen(String countryCode, Integer uid, String name, Short age, Integer sal, Boolean isPassportHolder, Float f1, Double f2, Long f3, BigDecimal f4, Integer pincode, NavigableMap<Long, Integer> phoneNumberHistory, Map<String, Integer> extraFlags, Dependents dependents) {
        this.countryCode = countryCode;
        this.uid = uid;
        this.name = name;
        this.phoneNumberHistory = phoneNumberHistory;
        this.extraFlags = extraFlags;
        this.dependents = dependents;
        this.nameInUpper = name == null ? null : name.toUpperCase();
        this.age = age;
        this.sal = sal;
        this.isPassportHolder = isPassportHolder;
        this.f1 = f1;
        this.f2 = f2;
        this.f3 = f3;
        this.f4 = f4;
        this.pincode = pincode;
    }

    public byte[] composeRowKey() throws HBRowKeyFieldCantBeNullException{
        // Check for null row key values. Nulls should produce HBRowKeyFieldCantBeNullException
        if(countryCode == null) {
            throw new HBRowKeyFieldCantBeNullException("Row key field countryCode can't be null");
        }

        if(uid == null) {
            throw new HBRowKeyFieldCantBeNullException("Row key field uid can't be null");
        }

        byte[] key = null;
        List<byte[]> parts = new ArrayList<byte[]>();
        parts.add(countryCode.getBytes());
        parts.add(KEY_DELIM.getBytes());
        parts.add(intToByteArray(uid));

        for(byte[] part : parts) {
            key = ArrayUtils.addAll(key, part);
        }
        return key;
    }

    public void parseRowKey(byte[] rowKeyBytes) {
        List<byte[]> tokens = splitTokens(rowKeyBytes, KEY_DELIM.getBytes());
        final int nTokens = tokens.size();
        if(nTokens == 2) {
            this.countryCode = Bytes.toString(tokens.get(0));
            this.uid = Bytes.toInt(tokens.get(1));
        } else {
            String msg = "Parsed " + nTokens + " tokens";
            throw new RowKeyCouldNotBeParsedException(msg, null);
        }
    }

    // Getter methods:

    public String getCountryCode() {
        return countryCode;
    }

    public Integer getUid() {
        return uid;
    }

    public String getName() {
        return name;
    }

    public Integer getSal() {
        return sal;
    }

    public Boolean isPassportHolder() {
        return isPassportHolder;
    }

    public Float getF1() {
        return f1;
    }

    public Double getF2() {
        return f2;
    }

    public Long getF3() {
        return f3;
    }

    public BigDecimal getF4() {
        return f4;
    }

    public Integer getPincode() {
        return pincode;
    }

    public Short getAge() {
        return age;
    }

    public Map<String, Integer> getExtraFlags() {
        return extraFlags;
    }

    public Dependents getDependents() {
        return dependents;
    }

    public NavigableMap<Long, Integer> getPhoneNumberHistory() {
        return phoneNumberHistory;
    }
}
