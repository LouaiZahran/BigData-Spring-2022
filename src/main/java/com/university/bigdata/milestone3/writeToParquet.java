package com.university.bigdata.milestone3;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.avro.AvroParquetWriter;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;

public class writeToParquet {
    public static void main(String[] args) {
        Schema schema = parseSchema();
        List<GenericData.Record> recordList = createRecords(schema);
        writeToParquetFile(recordList, schema);
    }

    // Method to parse the schema
    private static Schema parseSchema() {
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
    public static void addToRecord(int recordNumber, String name, double value){

    }
    private static List<GenericData.Record> createRecords(Schema schema){
        List<GenericData.Record> recordList = new ArrayList<>();
        for(int i = 1; i <= 15; i++) {
            GenericData.Record record = new GenericData.Record(schema);
            if(i%2 == 0)record.put("time", i);
            record.put("service", i*15);
            if(i%2 == 1)record.put("time", i);
            record.put("count", i);
            record.put("CPU", i*.5);
            record.put("RAM", i);
            record.put("DISK", i);
            record.put("CPU_MAX", i);
            record.put("RAM_MAX", i);
            record.put("DISK_MAX", i);

            recordList.add(record);
        }
        return recordList;
    }

    private static void writeToParquetFile(List<GenericData.Record> recordList, Schema schema) {
        // Output path for Parquet file in HDFS
        Path path =	new	Path("data.parquet");
        ParquetWriter<GenericData.Record> writer = null;
        // Creating ParquetWriter using builder
        try {
            writer = AvroParquetWriter.
                    <GenericData.Record>builder(path)
                    .withRowGroupSize(ParquetWriter.DEFAULT_BLOCK_SIZE)
                    .withPageSize(ParquetWriter.DEFAULT_PAGE_SIZE)
                    .withSchema(schema)
                    .withConf(new Configuration())
                    .withCompressionCodec(CompressionCodecName.SNAPPY)
                    .withValidation(false)
                    .withDictionaryEncoding(false)
                    .build();
            // writing records
            for (GenericData.Record record : recordList) {
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