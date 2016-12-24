package org.maxkons.hadoop_snippets.parquet;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.avro.AvroParquetReader;
import org.apache.parquet.avro.AvroParquetWriter;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;

/*
    Example of reading writing Parquet in java without BigData tools.
 */
public class ParquetReaderWriterWithAvro {

    private static final Logger LOGGER = LoggerFactory.getLogger(ParquetReaderWriterWithAvro.class);

    private static final Schema SCHEMA;
    private static final String SCHEMA_LOCATION = "/org/maxkons/hadoop_snippets/parquet/avroToParquet.avsc";
    private static final Path OUT_PATH = new Path("/home/max/Downloads/sample.parquet");

    static {
        try (InputStream inStream = ParquetReaderWriterWithAvro.class.getResourceAsStream(SCHEMA_LOCATION)) {
            SCHEMA = new Schema.Parser().parse(IOUtils.toString(inStream, "UTF-8"));
        } catch (IOException e) {
            LOGGER.error("Can't read SCHEMA file from {}", SCHEMA_LOCATION);
            throw new RuntimeException("Can't read SCHEMA file from" + SCHEMA_LOCATION, e);
        }
    }

    public static void main(String[] args) throws IOException {
        List<GenericData.Record> sampleData = new ArrayList<>();

        GenericData.Record record = new GenericData.Record(SCHEMA);
        record.put("c1", 1);
        record.put("c2", "someString");
        sampleData.add(record);

        record = new GenericData.Record(SCHEMA);
        record.put("c1", 2);
        record.put("c2", "otherString");
        sampleData.add(record);

        ParquetReaderWriterWithAvro writerReader = new ParquetReaderWriterWithAvro();
        writerReader.writeToParquet(sampleData, OUT_PATH);
        writerReader.readFromParquet(OUT_PATH);
    }

    @SuppressWarnings("unchecked")
    public void readFromParquet(Path filePathToRead) throws IOException {
        try (ParquetReader<GenericData.Record> reader = AvroParquetReader
                .<GenericData.Record>builder(filePathToRead)
                .withConf(new Configuration())
                .build()) {

            GenericData.Record record;
            while ((record = reader.read()) != null) {
                System.out.println(record);
            }
        }
    }

    public void writeToParquet(List<GenericData.Record> recordsToWrite, Path fileToWrite) throws IOException {
        try (ParquetWriter<GenericData.Record> writer = AvroParquetWriter
                .<GenericData.Record>builder(fileToWrite)
                .withSchema(SCHEMA)
                .withConf(new Configuration())
                .withCompressionCodec(CompressionCodecName.SNAPPY)
                .build()) {

            for (GenericData.Record record : recordsToWrite) {
                writer.write(record);
            }
        }
    }

}
