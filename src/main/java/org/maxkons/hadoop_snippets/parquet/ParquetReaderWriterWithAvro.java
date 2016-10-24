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

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;

/*
    Example of reading writing Parquet from java without BigData tools.
 */
public class ParquetReaderWriterWithAvro {

    public static final Schema schema;
    public static final Path OUT_PATH = new Path("/home/max/Downloads/sample.parquet");

    static {
        try (InputStream inStream = ParquetReaderWriterWithAvro.class.getResourceAsStream("/org/maxkons/hadoop_snippets/parquet/avroToParquet.avsc")) {
            schema = new Schema.Parser().parse(IOUtils.toString(inStream, "UTF-8"));
        } catch (IOException e) {
            throw new RuntimeException("Can't read schema file", e);
        }
    }

    public static void main(String[] args) throws IOException {
        List<GenericData.Record> sampleData = new ArrayList<>();

        GenericData.Record record = new GenericData.Record(schema);
        record.put("c1", 1);
        record.put("c2", "someString");
        sampleData.add(record);

        record = new GenericData.Record(schema);
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
                .withSchema(schema)
                .withConf(new Configuration())
                .withCompressionCodec(CompressionCodecName.SNAPPY)
                .build()) {

            for (GenericData.Record record : recordsToWrite) {
                writer.write(record);
            }
        }
    }

}
