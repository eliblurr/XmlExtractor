package com.brevanhoward.kafka.connect.smt;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

public class XmlExtractorTest {

    private String xmlStringData;
    private Map<String, Object> xmlMapData;
    private static final ObjectMapper objectMapper = new ObjectMapper();
    private SourceRecord stringSourceRecord;
    private SourceRecord structSourceRecord;
    private SourceRecord mapSourceRecord;
    private SourceRecord invalidSourceRecord;
    private Map<String, String> props;
    private XmlExtractor.Key<SourceRecord> xFormKey = new XmlExtractor.Key<>();
    private XmlExtractor.Value<SourceRecord> xFormValue = new XmlExtractor.Value<>();

    @Before
    public void setUp() throws IOException {
        try{
            URI xmlUri = getClass().getClassLoader().getResource("sample.xml").toURI();
            URI jsonUri = getClass().getClassLoader().getResource("sample.json").toURI();

            Path xmlPath = Paths.get(xmlUri);
            xmlStringData = new String(Files.readAllBytes(xmlPath));

            xmlMapData = objectMapper.readValue(new File(jsonUri), Map.class);
        } catch (Exception e) {
            e.printStackTrace();
        }

        props = new HashMap<String, String>() {{
            put("keys", "Customers.Customer.ContactName<ContactName>,Customers.Customer.ContactTitle<ContactTitle>,Customers.Customer.ContactName,trades.trade.portfolios.portfolio.portfolioLabel<labels><.*><_[^_].*$>,trades.trade.portfolios.portfolio.portfolioLabel<books><.*><^[^_]+>,Customers.Customer");
            put("keys.delimiter.regex", "\\.");
            put("xml.map.key", "blob");
        }};

        xFormValue.configure(props);
        xFormKey.configure(props);

        Schema recordSchema = SchemaBuilder.struct().name("Record")
                .field("blob", Schema.STRING_SCHEMA)
                .field("books", Schema.STRING_SCHEMA)
                .field("labels", Schema.STRING_SCHEMA)
                .field("Customers.Customer.ContactName", Schema.STRING_SCHEMA)
                .field("Customers.Customer", Schema.STRING_SCHEMA)
                .build();

        stringSourceRecord = new SourceRecord(null, null, null,
                null, null, xmlStringData, null, xmlStringData, null, null);

        mapSourceRecord = new SourceRecord(null, null, null,
                null, null, xmlMapData, null, xmlMapData, null, null);

        invalidSourceRecord = new SourceRecord(null, null, null,
                null, null, new ArrayList<>(), null, new ArrayList<>(), null, null);

        structSourceRecord = new SourceRecord(null, null, null,
                null, recordSchema, new Struct(recordSchema).put("blob", xmlStringData), recordSchema, new Struct(recordSchema).put("blob", xmlStringData), null, null);

    }

    @Test
    public void xmlExtractor_ValidateTransformOutputIsMap_GivenValidStringInputAtKey() {
        SourceRecord transformedRecord = xFormKey.apply(stringSourceRecord);
        assertEquals(transformedRecord.value(), stringSourceRecord.value());
        assertFalse(transformedRecord.key()==stringSourceRecord.key());
        assertTrue(transformedRecord.key() instanceof Map);
    }

    @Test
    public void xmlExtractor_ValidateTransformOutputIsMap_GivenValidStringInputAtValue() {
        SourceRecord transformedRecord = xFormValue.apply(stringSourceRecord);
        assertEquals(transformedRecord.key(), stringSourceRecord.key());
        assertFalse(transformedRecord.value()==stringSourceRecord.value());
        assertTrue(transformedRecord.value() instanceof Map);
    }

    @Test
    public void xmlExtractor_ValidateTransformOutputIsMap_GivenValidStructInputAtKey() {
        SourceRecord transformedRecord = xFormKey.apply(structSourceRecord);
        assertEquals(transformedRecord.value(), structSourceRecord.value());
        assertFalse(transformedRecord.key()==structSourceRecord.key());
        assertTrue(transformedRecord.key() instanceof Map);
    }

    @Test
    public void xmlExtractor_ValidateTransformOutputIsStruct_GivenValidStructInputAtValue() {
        SourceRecord transformedRecord = xFormValue.apply(structSourceRecord);
        assertEquals(transformedRecord.key(), structSourceRecord.key());
        assertFalse(transformedRecord.value()==structSourceRecord.value());
        assertTrue(transformedRecord.value() instanceof Map);
    }

    @Test
    public void xmlExtractor_ValidateTransformOutputIsMap_GivenMapInputAtKey() {
        SourceRecord transformedRecord = xFormKey.apply(mapSourceRecord);
        assertEquals(transformedRecord.value(), mapSourceRecord.value());
        assertTrue(((Map)transformedRecord.key()).keySet().containsAll(((Map)mapSourceRecord.key()).keySet()));
        assertTrue(transformedRecord.key() instanceof Map);
    }

    @Test
    public void xmlExtractor_ValidateTransformOutputIsMap_GivenMapInputAtValue() {
        SourceRecord transformedRecord = xFormValue.apply(mapSourceRecord);
        assertEquals(transformedRecord.key(), mapSourceRecord.key());
        assertTrue(((Map)transformedRecord.value()).keySet().containsAll(((Map)mapSourceRecord.value()).keySet()));
        assertTrue(transformedRecord.value() instanceof Map);
    }

    @Test
    public void xmlExtractor_Config_ValidateApplyIsCalled() {
        XmlExtractor.Key<SourceRecord> xFormKeyMock = Mockito.mock(XmlExtractor.Key.class);
        XmlExtractor.Value<SourceRecord> xFormValueMock = Mockito.mock(XmlExtractor.Value.class);

        xFormKeyMock.apply(mapSourceRecord);
        xFormKeyMock.apply(stringSourceRecord);
        xFormKeyMock.apply(structSourceRecord);


        xFormValueMock.apply(mapSourceRecord);
        xFormValueMock.apply(stringSourceRecord);
        xFormValueMock.apply(structSourceRecord);

        verify(xFormKeyMock, times(1)).apply(mapSourceRecord);
        verify(xFormKeyMock, times(1)).apply(stringSourceRecord);
        verify(xFormKeyMock, times(1)).apply(structSourceRecord);

        verify(xFormValueMock, times(1)).apply(mapSourceRecord);
        verify(xFormValueMock, times(1)).apply(stringSourceRecord);
        verify(xFormValueMock, times(1)).apply(structSourceRecord);
    }

    @Test
    public void xmlExtractor_Config_ValidateKafkaRecordFieldToUse() {
        assertEquals(xFormValue.operatingValue(mapSourceRecord), mapSourceRecord.value());
        assertEquals(xFormValue.operatingValue(stringSourceRecord), stringSourceRecord.value());
        assertEquals(xFormValue.operatingValue(structSourceRecord), structSourceRecord.value());

        assertEquals(xFormKey.operatingValue(mapSourceRecord), mapSourceRecord.value());
        assertEquals(xFormKey.operatingValue(stringSourceRecord), stringSourceRecord.value());
        assertEquals(xFormKey.operatingValue(structSourceRecord), structSourceRecord.value());
    }

    @Test
    public void xmlExtractor_ValidateTransformOutputHasAllKeys_GivenMapInputAtKey() {
        Map<?,?> record = (Map) xFormKey.apply(mapSourceRecord).value();
        assertFalse(record.containsKey("Customers.Customer.ContactTitle"));
        assertTrue(record.containsKey("Customers.Customer.ContactName"));
        assertTrue(record.containsKey("ContactName"));
        assertTrue(record.containsKey("ContactTitle"));
        assertTrue(record.containsKey("test_key"));
        assertTrue(record.containsKey("blob"));
    }

    @Test
    public void xmlExtractor_ValidateTransformOutputHasAllKeys_GivenMapInputAtValue() {
        Map<?,?> record = (Map) xFormValue.apply(mapSourceRecord).value();
        assertFalse(record.containsKey("Customers.Customer.ContactTitle"));
        assertTrue(record.containsKey("Customers.Customer.ContactName"));
        assertTrue(record.containsKey("ContactName"));
        assertTrue(record.containsKey("ContactTitle"));
        assertTrue(record.containsKey("test_key"));
        assertTrue(record.containsKey("blob"));
    }

    @Test
    public void xmlExtractor_ValidateTransformOutputHasAllKeys_GivenStructInputAtKey() {
        Map<?,?> record = (Map) xFormKey.apply(structSourceRecord).key();
        assertFalse(record.containsKey("Customers.Customer.ContactTitle"));
        assertTrue(record.containsKey("Customers.Customer.ContactName"));
        assertTrue(record.containsKey("ContactName"));
        assertTrue(record.containsKey("ContactTitle"));
        assertTrue(record.containsKey("blob"));
    }

    @Test
    public void xmlExtractor_ValidateTransformOutputHasAllKeys_GivenStructInputAtValue() {
        Map<?,?> record = (Map) xFormValue.apply(structSourceRecord).value();
        assertFalse(record.containsKey("Customers.Customer.ContactTitle"));
        assertTrue(record.containsKey("Customers.Customer.ContactName"));
        assertTrue(record.containsKey("ContactName"));
        assertTrue(record.containsKey("ContactTitle"));
        assertTrue(record.containsKey("blob"));
    }

    @Test
    public void xmlExtractor_ValidateTransformOutputHasAllKeys_GivenStringInputAKey() {
        Map<?,?> record = (Map) xFormKey.apply(stringSourceRecord).key();
        assertFalse(record.containsKey("Customers.Customer.ContactTitle"));
        assertTrue(record.containsKey("Customers.Customer.ContactName"));
        assertTrue(record.containsKey("ContactName"));
        assertTrue(record.containsKey("ContactTitle"));
        assertTrue(record.containsKey("blob"));
    }

    @Test
    public void xmlExtractor_ValidateTransformOutputHasAllKeys_GivenStringInputAtValue() {
        Map<?,?> record = (Map) xFormValue.apply(stringSourceRecord).value();
        assertFalse(record.containsKey("Customers.Customer.ContactTitle"));
        assertTrue(record.containsKey("Customers.Customer.ContactName"));
        assertTrue(record.containsKey("ContactName"));
        assertTrue(record.containsKey("ContactTitle"));
        assertTrue(record.containsKey("blob"));
    }

    @Test
    public void xmlExtractor_ValidateTransformOutputIsSameAsInput_GivenInvalidInputType() {
        assertEquals(xFormKey.operatingValue(invalidSourceRecord), invalidSourceRecord.key());
        assertEquals(xFormValue.operatingValue(invalidSourceRecord), invalidSourceRecord.value());
    }

    @Test
    public void xmlExtractor_ValidateTransformOutputIsSameAsInput_GivenInvalidKeyInputType_AfterApply() {
        SourceRecord transformedRecord = xFormKey.apply(invalidSourceRecord);
        assertEquals(transformedRecord.key(), invalidSourceRecord.key());
        assertEquals(transformedRecord.key().getClass(), invalidSourceRecord.key().getClass());
    }

    @Test
    public void xmlExtractor_ValidateTransformOutputIsSameAsInput_GivenInvalidValueInputType_AfterApply() {
        SourceRecord transformedRecord = xFormValue.apply(invalidSourceRecord);
        assertEquals(transformedRecord.value(), invalidSourceRecord.value());
        assertEquals(transformedRecord.value().getClass(), invalidSourceRecord.value().getClass());
    }

    @After
    public void tearDown() throws Exception {
        xFormValue.close();
        xFormKey.close();
        xmlMapData.clear();
    }
}
