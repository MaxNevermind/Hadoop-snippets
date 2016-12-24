package org.maxkons.hadoop_snippets.avro;


import org.apache.avro.Schema;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;

/*
    Example of writing Avro in java without BigData tools.
 */
public class AvroWriter {

    private static final Logger LOGGER = LoggerFactory.getLogger(AvroWriter.class);

    private static final Schema SCHEMA;
    private static final String SCHEMA_LOCATION = "/org/maxkons/hadoop_snippets/avro/sample.avsc";
    private static final Path OUT_PATH = new Path("/home/max/Downloads/sample.avro");

    static {
        try (InputStream inStream = AvroWriter.class.getResourceAsStream(SCHEMA_LOCATION)) {
            SCHEMA = new Schema.Parser().parse(IOUtils.toString(inStream, "UTF-8"));
        } catch (IOException e) {
            LOGGER.error("Can't read SCHEMA file from {}", SCHEMA_LOCATION);
            throw new RuntimeException("Can't read SCHEMA file from " + SCHEMA_LOCATION, e);
        }
    }

    public static void main(String[] args) throws IOException {
        FileSystem fileSystem = FileSystem.newInstance(new Configuration());
        try (DataFileWriter<GenericRecord> genericRecordDataFileWriter = new DataFileWriter(new GenericDatumWriter<>()).create(SCHEMA, fileSystem.create(OUT_PATH))) {
            List<GenericData.Record> records = getData();
            for (GenericData.Record record: records) {
                genericRecordDataFileWriter.append(record);
            }
        }
    }

    public static List<GenericData.Record> getData() {
        List<GenericData.Record> records = new ArrayList<>();

        GenericData.Record parentRecord = new GenericData.Record(SCHEMA);
        parentRecord.put("requiredField1", "I");
        parentRecord.put("requiredField2", "2016-06-28 11:54:55.010163");

        GenericData.Record childRecord = new GenericData.Record(parentRecord.getSchema().getField("childRecord").schema().getTypes().get(1));
        childRecord.put("ID", 1L);
        childRecord.put("c1", "string");

        parentRecord.put("childRecord", childRecord);
        records.add(parentRecord);

        parentRecord = new GenericData.Record(SCHEMA);
        parentRecord.put("requiredField1", "U");
        parentRecord.put("requiredField2", "2016-06-28 11:54:55.010163");

        childRecord = new GenericData.Record(parentRecord.getSchema().getField("childRecord").schema().getTypes().get(1));
        childRecord.put("ID", 2L);
        childRecord.put("c1", "string");

        parentRecord.put("childRecord", childRecord);
        records.add(parentRecord);

        return records;
    }


}
