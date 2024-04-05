## Xml Extractor Custom Transformer

Kafka Connect SMT to parse Read Specific XML keys and parse to JSON.

Only supports schemaless
Properties:

| Name                   | Description                                                                                                                                          | Type           | Default | Importance  |
|------------------------|------------------------------------------------------------------------------------------------------------------------------------------------------|----------------|---------|-------------|
| `keys`                 | Comma separated string of list of keys to extract, add alias for key field in box braces                                                             | String         |         | High        |
| `keys.delimiter.regex` | Delimiter string to a single key provided for nested key                                                                                             | String         |         | High        |
| `xml.map.key`          | Field name in resulting map to hold original xml for <String> record inputs or Field containing JSON key that holds XML data for <Map> records input | String         | `_xml_blob_` | High |



Example on how to add to your connector:
```
transforms=xmlExtractor
transforms.xmlExtractor.type=com.brevanhoward.kafka.connect.smt.xmlExtractor
transforms.xmlExtractor.keys.delimiter.regex=\\.
transforms.xmlExtractor.xml.map.key=xmlBlob
transforms.xmlExtractor.keys=Customers.Customer.ContactName<AliasForContactName>,Customers.Customer.ContactName, Customers.Customer.ContactTitle
```

ToDO
* ~~add support for records with schemas[test for structs]~~
* developer notes
  * ```else if (value instanceof Struct) {
            Struct structRecord = (Struct) value;
            if (structRecord.schema().field(xmlDataKey) != null) { throw new InvalidConfigurationException(""); }
            processedRecord = processXml(structRecord.getString(xmlDataKey), keyFieldNames, keyFieldDelimiter);
            processedRecord.forEach((key, val) -> {
                if (structRecord.schema().field(key) != null) {
                    structRecord.put(key, value);
                }
            });
            return newRecord(record, null, structRecord);
        } ```
  * ```throw new SerializationException("Error serializing message, unexpected type passed to transformer"); ```
  * ```@Test
    public void xmlExtractor_ValidateTransformThrowsException_GivenMapInput_WithMissingMapKey() {
        SourceRecord record = new SourceRecord(null, null, null,
                null, null, new HashMap<>(), null, new HashMap<>(), null, null);
        assertThrows(InvalidConfigurationException.class, () -> xFormValue.apply(record));
        assertThrows(InvalidConfigurationException.class, () -> xFormKey.apply(record));
    }

    @Test
    public void xmlExtractor_ValidateTransformThrowsException_GivenMapInput_WithInvalidType() {
        SourceRecord record = new SourceRecord(null, null, null,
                null, null, 2, null, 2, null, null);
        assertThrows(SerializationException.class, () -> xFormValue.apply(record));
        assertThrows(SerializationException.class, () -> xFormKey.apply(record));
    }```
    
