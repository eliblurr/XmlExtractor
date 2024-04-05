package com.brevanhoward.kafka.connect.smt;

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
import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

public class XmlExtractorTest {

    private String xmlStringData;
    private Map<String, Object> xmlMapData;
    private static final ObjectMapper objectMapper = new ObjectMapper();
    private SourceRecord stringSourceRecord;
    private SourceRecord mapSourceRecord;
    private SourceRecord stringNonXmlSourceRecord;
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
            put("keys", "Customers.Customer.ContactName<ContactName>,Customers.Customer.ContactTitle<ContactTitle>,Customers.Customer.ContactName,MxML.trades.trade.portfolios.portfolio.portfolioLabel<labels><.*><_[^_].*$>,MxML.trades.trade.portfolios.portfolio.portfolioLabel<books><.*><^[^_]+>,Customers.Customer");
            put("keys.delimiter.regex", "\\.");
            put("xml.map.key", "blob");
        }};

        xFormValue.configure(props);
        xFormKey.configure(props);

        stringSourceRecord = new SourceRecord(null, null, null,
                null, null, xmlStringData, null, xmlStringData, null, null);

        mapSourceRecord = new SourceRecord(null, null, null,
                null, null, xmlMapData, null, xmlMapData, null, null);

        stringNonXmlSourceRecord = new SourceRecord(null, null, null,
                null, null, "bad string format", null, "bad string format", null, null);
    }

    @Test
    public void xmlExtractor_ValidateTransformOutputIsMap_GivenStringInputAtKey() {
        SourceRecord transformedRecord = xFormKey.apply(stringSourceRecord);
        assertEquals(transformedRecord.value(), stringSourceRecord.value());
        assertFalse(transformedRecord.key()==stringSourceRecord.key());
        assertTrue(transformedRecord.key() instanceof Map);
    }

    @Test
    public void xmlExtractor_ValidateTransformOutputIsMap_GivenStringInputAtValue() {
        SourceRecord transformedRecord = xFormValue.apply(stringSourceRecord);
        assertEquals(transformedRecord.key(), stringSourceRecord.key());
        assertFalse(transformedRecord.value()==stringSourceRecord.value());
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

        xFormValueMock.apply(mapSourceRecord);
        xFormValueMock.apply(stringSourceRecord);

        verify(xFormKeyMock, times(1)).apply(mapSourceRecord);
        verify(xFormKeyMock, times(1)).apply(stringSourceRecord);

        verify(xFormValueMock, times(1)).apply(mapSourceRecord);
        verify(xFormValueMock, times(1)).apply(stringSourceRecord);
    }

    @Test
    public void xmlExtractor_Config_ValidateKafkaRecordFieldToUse() {
        assertEquals(xFormValue.operatingValue(mapSourceRecord), mapSourceRecord.value());
        assertEquals(xFormValue.operatingValue(stringSourceRecord), stringSourceRecord.value());

        assertEquals(xFormKey.operatingValue(mapSourceRecord), mapSourceRecord.value());
        assertEquals(xFormKey.operatingValue(stringSourceRecord), stringSourceRecord.value());
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
    public void xmlExtractor_ValidateTransformOutputIsSameAsInput_GivenNonXmlInput() {
        assertEquals(xFormKey.operatingValue(stringNonXmlSourceRecord), stringNonXmlSourceRecord.key());
        assertEquals(xFormValue.operatingValue(stringNonXmlSourceRecord), stringNonXmlSourceRecord.value());
    }

    @After
    public void tearDown() throws Exception {
        xFormValue.close();
        xFormKey.close();
    }
}
