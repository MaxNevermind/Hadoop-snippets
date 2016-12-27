package org.maxkons.hadoop_snippets.avro;

import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.AvroFSInput;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;

/*
    Example of reading Avro in java without BigData tools.
 */
public class AvroReader {

    private static final Path IN_PATH = new Path("/home/max/Downloads/sample.avro");

    public static void main(String[] args) throws IOException {
        FileSystem fileSystem = FileSystem.newInstance(new Configuration());
        try (DataFileReader<GenericRecord> dataFileReader = new DataFileReader<>(new AvroFSInput(fileSystem.open(IN_PATH), 0), new GenericDatumReader<>())) {
            for (GenericRecord datum : dataFileReader) {
                System.out.println(datum);
            }
        }
    }

}
