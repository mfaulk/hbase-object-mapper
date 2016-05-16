package com.flipkart.hbaseobjectmapper;

import com.flipkart.hbaseobjectmapper.entities.*;
import com.flipkart.hbaseobjectmapper.exceptions.*;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.javatuples.Triplet;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.util.*;

import static com.flipkart.hbaseobjectmapper.TestUtil.triplet;
import static org.junit.Assert.*;

public class TestHBObjectMapper {
    List<Triplet<HBRecord, String, Class<? extends IllegalArgumentException>>> invalidRecordsAndErrorMessages = Arrays.asList(
            triplet(Singleton.getInstance(), "A singleton class", EmptyConstructorInaccessibleException.class),
            triplet(new ClassWithNoEmptyConstructor(1), "Class with no empty constructor", NoEmptyConstructorException.class),
            triplet(new ClassWithPrimitives(1f), "A class with primitives", MappedColumnCantBePrimitiveException.class),
            triplet(new ClassWithTwoFieldsMappedToSameColumn(), "Class with two fields mapped to same column", FieldsMappedToSameColumnException.class),
            triplet(new ClassWithBadAnnotationStatic(), "Class with a static field mapped to HBase column", MappedColumnCantBeStaticException.class),
            triplet(new ClassWithBadAnnotationTransient("James", "Gosling"), "Class with a transient field mapped to HBase column", MappedColumnCantBeTransientException.class),
            triplet(new ClassWithNoHBColumns(), "Class with no fields mapped with HBColumn", MissingHBColumnFieldsException.class),
            triplet(new ClassWithNoHBRowKeys(), "Class with no fields mapped with HBRowKey", MissingHBRowKeyFieldsException.class),
            triplet(new ClassesWithFieldIncomptibleWithHBColumnMultiVersion.NotMap(), "Class with an incompatible field (not Map) annotated with " + HBColumnMultiVersion.class.getName(), IncompatibleFieldForHBColumnMultiVersionAnnotationException.class),
            triplet(new ClassesWithFieldIncomptibleWithHBColumnMultiVersion.NotNavigableMap(), "Class with an incompatible field (not NavigableMap) annotated with " + HBColumnMultiVersion.class.getName(), IncompatibleFieldForHBColumnMultiVersionAnnotationException.class),
            triplet(new ClassesWithFieldIncomptibleWithHBColumnMultiVersion.EntryKeyNotLong(), "Class with an incompatible field (NavigableMap's entry key not Long) annotated with " + HBColumnMultiVersion.class.getName(), IncompatibleFieldForHBColumnMultiVersionAnnotationException.class)
    );

    HBObjectMapper hbMapper = new HBObjectMapper();
    List<Citizen> validObjs = TestObjects.validObjs;

    Result someResult = hbMapper.writeValueAsResult(validObjs.get(0));
    Put somePut = hbMapper.writeValueAsPut(validObjs.get(0));

    @Rule
    public ExpectedException thrown = ExpectedException.none();

    @Test
    public void testHBObjectMapper() {
        for (Citizen obj : validObjs) {
            System.out.printf("Original object: %s%n", obj);
            testResult(obj);
            testResultWithRow(obj);
            testPut(obj);
            testPutWithRow(obj);
        }
    }

    public void testResult(HBRecord p) {
        long start, end;
        start = System.currentTimeMillis();
        Result result = hbMapper.writeValueAsResult(p);
        end = System.currentTimeMillis();
        System.out.printf("Time taken for POJO->Result = %dms%n", end - start);
        start = System.currentTimeMillis();
        Citizen pFromResult = hbMapper.readValue(result, Citizen.class);
        end = System.currentTimeMillis();
        assertEquals("Data mismatch after deserialization from Result", p, pFromResult);
        System.out.printf("Time taken for Result->POJO = %dms%n%n", end - start);
    }

    public void testResultWithRow(HBRecord p) {
        long start, end;
        Result result = hbMapper.writeValueAsResult(Arrays.asList(p)).get(0);
        ImmutableBytesWritable rowKey = new ImmutableBytesWritable(p.composeRowKey());
        start = System.currentTimeMillis();
        Citizen pFromResult = hbMapper.readValue(rowKey, result, Citizen.class);
        end = System.currentTimeMillis();
        assertEquals("Data mismatch after deserialization from Result+Row", p, pFromResult);
        System.out.printf("Time taken for Result+Row->POJO = %dms%n%n", end - start);
    }

    public void testPut(HBRecord p) {
        long start, end;
        start = System.currentTimeMillis();
        Put put = hbMapper.writeValueAsPut(Arrays.asList(p)).get(0);
        end = System.currentTimeMillis();
        System.out.printf("Time taken for POJO->Put = %dms%n", end - start);
        start = System.currentTimeMillis();
        Citizen pFromPut = hbMapper.readValue(put, Citizen.class);
        end = System.currentTimeMillis();
        assertEquals("Data mismatch after deserialization from Put", p, pFromPut);
        System.out.printf("Time taken for Put->POJO = %dms%n%n", end - start);
    }

    public void testPutWithRow(HBRecord p) {
        long start, end;
        Put put = hbMapper.writeValueAsPut(p);
        ImmutableBytesWritable rowKey = new ImmutableBytesWritable(p.composeRowKey());
        start = System.currentTimeMillis();
        Citizen pFromPut = hbMapper.readValue(rowKey, put, Citizen.class);
        end = System.currentTimeMillis();
        assertEquals("Data mismatch after deserialization from Put", p, pFromPut);
        System.out.printf("Time taken for Put->POJO = %dms%n%n", end - start);
    }

    @Test
    public void testInvalidRowKey() {
        Citizen e = TestObjects.validObjs.get(0);
        thrown.expect(RowKeyCouldNotBeParsedException.class);
        hbMapper.readValue(new RowKey("invalid row key".getBytes()), hbMapper.writeValueAsPut(e), Citizen.class);
    }

    @Test
    public void testValidClasses() {
        assertTrue(hbMapper.isValid(Citizen.class));
        assertTrue(hbMapper.isValid(CitizenSummary.class));
    }

    @Test
    public void testInvalidClasses() {
        Set<String> exceptionMessages = new HashSet<String>();
        for (Triplet<HBRecord, String, Class<? extends IllegalArgumentException>> p : invalidRecordsAndErrorMessages) {
            HBRecord record = p.getValue0();
            Class recordClass = record.getClass();
            assertFalse("Object mapper couldn't detect invalidity of class " + recordClass.getName(), hbMapper.isValid(recordClass));
            String errorMessage = p.getValue1() + " (" + recordClass.getName() + ") should have thrown an " + IllegalArgumentException.class.getName();
            String exMsgObjToResult = null, exMsgObjToPut = null, exMsgResultToObj = null, exMsgPutToObj = null;
            try {
                hbMapper.writeValueAsResult(record);
                fail(errorMessage + " while converting bean to Result");
            } catch (IllegalArgumentException ex) {
                assertEquals("Mismatch in type of exception thrown for " + recordClass.getSimpleName(), p.getValue2(), ex.getClass());
                exMsgObjToResult = ex.getMessage();
            }
            try {
                hbMapper.writeValueAsPut(record);
                fail(errorMessage + " while converting bean to Put");
            } catch (IllegalArgumentException ex) {
                assertEquals("Mismatch in type of exception thrown for " + recordClass.getSimpleName(), p.getValue2(), ex.getClass());
                exMsgObjToPut = ex.getMessage();
            }
            try {
                hbMapper.readValue(someResult, recordClass);
                fail(errorMessage + " while converting Result to bean");
            } catch (IllegalArgumentException ex) {
                assertEquals("Mismatch in type of exception thrown for " + recordClass.getSimpleName(), p.getValue2(), ex.getClass());
                exMsgResultToObj = ex.getMessage();
            }
            try {
                hbMapper.readValue(new ImmutableBytesWritable(someResult.getRow()), someResult, recordClass);
                fail(errorMessage + " while converting Result to bean");
            } catch (IllegalArgumentException ex) {
                assertEquals("Mismatch in type of exception thrown for " + recordClass.getSimpleName(), p.getValue2(), ex.getClass());
            }
            try {
                hbMapper.readValue(somePut, recordClass);
                fail(errorMessage + " while converting Put to bean");
            } catch (IllegalArgumentException ex) {
                assertEquals("Mismatch in type of exception thrown for " + recordClass.getSimpleName(), p.getValue2(), ex.getClass());
                exMsgPutToObj = ex.getMessage();
            }
            try {
                hbMapper.readValue(new ImmutableBytesWritable(somePut.getRow()), somePut, recordClass);
                fail(errorMessage + " while converting row key and Put combo to bean");
            } catch (IllegalArgumentException ex) {
                assertEquals("Mismatch in type of exception thrown", p.getValue2(), ex.getClass());
            }
            assertEquals("Validation for 'conversion to Result' and 'conversion to Put' differ in code path", exMsgObjToResult, exMsgObjToPut);
            assertEquals("Validation for 'conversion from Result' and 'conversion from Put' differ in code path", exMsgResultToObj, exMsgPutToObj);
            assertEquals("Validation for 'conversion from bean' and 'conversion to bean' differ in code path", exMsgObjToResult, exMsgResultToObj);
            System.out.printf("%s threw below Exception as expected:\n%s\n%n", p.getValue1(), exMsgObjToResult);
            if (!exceptionMessages.add(exMsgObjToPut)) {
                fail("Same error message for different invalid inputs");
            }
        }
    }


    @Test
    public void testNullRowKeyField() {
        // HBRecord with null row key field
        HBRecord citizen = new Citizen(null, -2, "row key field null 1", null, null, null, null, null, null, null, null, null, null, null);
        thrown.expect(HBRowKeyFieldCantBeNullException.class);
        hbMapper.writeValueAsResult(citizen);
    }

    @Test
    public void testInvalidObjs() {
        for (Triplet<HBRecord, String, Class<? extends IllegalArgumentException>> p : TestObjects.invalidObjs) {
            HBRecord record = p.getValue0();
            String errorMessage = "An object with " + p.getValue1() + " should've thrown an " + p.getValue2().getName();
            try {
                hbMapper.writeValueAsResult(record);
                fail(errorMessage + " while converting bean to Result\nFailing object = " + record);
            } catch (IllegalArgumentException ex) {
                assertEquals("Mismatch in type of exception thrown", p.getValue2(), ex.getClass());
            }
            try {
                hbMapper.writeValueAsPut(record);
                fail(errorMessage + " while converting bean to Put\nFailing object = " + record);
            } catch (IllegalArgumentException ex) {
                assertEquals("Mismatch in type of exception thrown", p.getValue2(), ex.getClass());
            }
        }
    }

    @Test
    public void testEmptyResults() {
        Result nullResult = null, emptyResult = new Result(), resultWithBlankRowKey = new Result(new ImmutableBytesWritable(new byte[]{}));
        Citizen nullCitizen = hbMapper.readValue(nullResult, Citizen.class);
        assertNull("Null Result object should return null", nullCitizen);
        Citizen emptyCitizen = hbMapper.readValue(emptyResult, Citizen.class);
        assertNull("Empty Result object should return null", emptyCitizen);
        assertNull(hbMapper.readValue(resultWithBlankRowKey, Citizen.class));
    }

    @Test
    public void testEmptyPuts() {
        Put nullPut = null, emptyPut = new Put(), putWithBlankRowKey = new Put(new byte[]{});
        Citizen nullCitizen = hbMapper.readValue(nullPut, Citizen.class);
        assertNull("Null Put object should return null", nullCitizen);
        Citizen emptyCitizen = hbMapper.readValue(emptyPut, Citizen.class);
        assertNull("Empty Put object should return null", emptyCitizen);
        assertNull(hbMapper.readValue(putWithBlankRowKey, Citizen.class));
    }

    @Test
    public void testGetRowKey() {
        ImmutableBytesWritable rowKey = hbMapper.getRowKey(new HBRecord() {
            @Override
            public byte[] composeRowKey() {
                return "rowkey".getBytes();
            }

            @Override
            public void parseRowKey(byte[] rowKey) {

            }
        });
        assertEquals("Row keys don't match", rowKey, Util.strToIbw("rowkey"));
        try {
            hbMapper.getRowKey(new HBRecord() {
                @Override
                public byte[] composeRowKey() {
                    return null;
                }

                @Override
                public void parseRowKey(byte[] rowKey) {

                }
            });
            fail("null row key should've thrown a " + RowKeyCantBeEmptyException.class.getName());
        } catch (RowKeyCantBeEmptyException ignored) {

        }
        try {
            hbMapper.getRowKey(new HBRecord() {
                @Override
                public byte[] composeRowKey() {
                    throw new RuntimeException("Some blah");
                }

                @Override
                public void parseRowKey(byte[] rowKey) {

                }
            });
            fail("If row key can't be composed, an " + RowKeyCantBeComposedException.class.getName() + " was expected");
        } catch (RowKeyCantBeComposedException ignored) {

        }
        try {
            hbMapper.getRowKey(null);
            fail("If object is null, a " + NullPointerException.class.getName() + " was expected");
        } catch (NullPointerException ignored) {

        }
    }

    @Test
    public void testUninstantiatableClass() {
        try {
            hbMapper.readValue(someResult, UninstantiatableClass.class);
            fail("If class can't be instantiated, a " + ObjectNotInstantiatableException.class.getName() + " was expected");
        } catch (ObjectNotInstantiatableException e) {

        }
    }

    @Test
    public void testHBColumnMultiVersion() {
        Double[] testNumbers = new Double[]{3.14159, 2.71828, 0.0};
        for (Double n : testNumbers) {
            // Written as unversioned, read as versioned
            Result result = hbMapper.writeValueAsResult(new CrawlNoVersion("key").setF1(n));
            Crawl versioned = hbMapper.readValue(result, Crawl.class);
            NavigableMap<Long, Double> columnHistory = versioned.getF1();
            assertEquals("Column history size mismatch", 1, columnHistory.size());
            assertEquals(String.format("Inconsistency between %s and %s", HBColumn.class.getSimpleName(), HBColumnMultiVersion.class.getSimpleName()), n, columnHistory.lastEntry().getValue());
            // Written as versioned, read as unversioned
            Result result1 = hbMapper.writeValueAsResult(new Crawl("key").addF1(Double.MAX_VALUE).addF1(Double.MAX_VALUE).addF1(Double.MAX_VALUE).addF1(n));
            CrawlNoVersion unversioned = hbMapper.readValue(result1, CrawlNoVersion.class);
            Double f1 = unversioned.getF1();
            assertEquals(String.format("Inconsistency between %s and %s", HBColumnMultiVersion.class.getSimpleName(), HBColumn.class.getSimpleName()), n, f1);
        }
    }
}
