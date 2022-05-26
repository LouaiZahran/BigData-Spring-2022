package com.university.bigdata.milestone3;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.avro.AvroParquetWriter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;

import java.io.File;
import java.io.IOException;
import java.util.List;

public class ParquetWriter {

    public static Schema parseSchema() {
        Schema.Parser parser = new	Schema.Parser();
        Schema schema = null;
        try {
            // Path to schema file
            schema = parser.parse(new File("src/main/java/com/university/bigdata/milestone3/schema.avsc"));
        } catch (IOException e) {
            e.printStackTrace();
        }

        return schema;
    }

    public static void writeToParquetFile(List<GenericData.Record> recordList, Schema schema) {
        File file = new File("realtime_view/data.parquet");
        if(file.exists())
            file.delete();
        // Output path for Parquet file in HDFS
        Path path =	new	Path("realtime_view/data.parquet");
        org.apache.parquet.hadoop.ParquetWriter<GenericData.Record> writer = null;
        // Creating ParquetWriter using builder
        try {
            writer = AvroParquetWriter.
                    <GenericData.Record>builder(path)
                    .withRowGroupSize(org.apache.parquet.hadoop.ParquetWriter.DEFAULT_BLOCK_SIZE)
                    .withPageSize(org.apache.parquet.hadoop.ParquetWriter.DEFAULT_PAGE_SIZE)
                    .withSchema(schema)
                    .withConf(new Configuration())
                    .withCompressionCodec(CompressionCodecName.SNAPPY)
                    .withValidation(false)
                    .withDictionaryEncoding(false)
                    .build();
            // writing records
            for (GenericData.Record record : recordList) {
                System.out.println(record);
                writer.write(record);
            }
        }catch(IOException e) {
            e.printStackTrace();
        }finally {
            if(writer != null) {
                try {
                    writer.close();
                } catch (IOException e) {
                    // TODO Auto-generated catch block
                    e.printStackTrace();
                }
            }
        }
    }

}
