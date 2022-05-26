package com.university.bigdata.milestone3;

import com.google.gson.Gson;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.avro.AvroParquetWriter;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.io.File;
import java.io.IOException;
import java.util.*;

public class SpeedLayer implements Obs{

    static Schema schema;
    static HashMap<String, GenericData.Record> records = new HashMap<>();
    static Gson parser = new Gson();

    private static String[] tokenize(String line){
        List<String> ret = new ArrayList<>();
        String[] messages = line.toString().split("}}");
        for (String message : messages) {
            message = message.concat("}}");
            ret.add(message);
        }
        return ret.toArray(new String[0]);
    }

    private static Iterator<Tuple2<String, String>> extractData(Info info){
        String name = info.serviceName;
        long time=(info.Timestamp - 1647938697)/60;
        List<Tuple2<String, String>> ret = new ArrayList<>();
        ret.add(new Tuple2<>(name + "_" + time + "_Count", "1"));
        ret.add(new Tuple2<>(name + "_" + time + "_CPU", String.valueOf(info.CPU)));
        ret.add(new Tuple2<>(name + "_" + time + "_CPU_MAX", String.valueOf(info.CPU)));
        ret.add(new Tuple2<>(name + "_" + time + "_Disk", String.valueOf(info.Disk.Free)));
        ret.add(new Tuple2<>(name + "_" + time + "_Disk_MAX", String.valueOf(info.Disk.Free)));
        ret.add(new Tuple2<>(name + "_" + time + "_RAM", String.valueOf(info.RAM.Free)));
        ret.add(new Tuple2<>(name + "_" + time + "_RAM_MAX", String.valueOf(info.RAM.Free)));
        return ret.iterator();
    }

    private static String reduce(String value1, String value2){
        double value1D = Double.parseDouble(value1);
        double value2D = Double.parseDouble(value2);
        double ret = value1D + value2D;
        return String.valueOf(ret);
    }

    private static String reduceMax(String value1, String value2){
        double value1D = Double.parseDouble(value1);
        double value2D = Double.parseDouble(value2);
        double ret = Math.max(value1D, value2D);
        return String.valueOf(ret);
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

    private static void addRecord(Tuple2<String, String> record){
        String key = record._1;
        String value = record._2;
        boolean isMax = key.contains("_MAX");       key = key.replace("_MAX", "");
        boolean isDisk = key.contains("_Disk");     key = key.replace("_Disk", "");
        boolean isRAM = key.contains("_RAM");       key = key.replace("_RAM", "");
        boolean isCPU = key.contains("_CPU");       key = key.replace("_CPU", "");
        boolean isCount = key.contains("_Count");   key = key.replace("_Count", "");

        int serviceNum = Integer.parseInt(key.split("_")[0].split("-")[1]);
        long timestamp = (long) Double.parseDouble(key.split("_")[1]);

        GenericData.Record record1 = records.getOrDefault(key, new GenericData.Record(schema));
        if(record1.get("service") == null)
            record1.put("service", serviceNum);
        if(record1.get("time") == null)
            record1.put("time", timestamp);
        if(isMax) {
            if(isCPU) record1.put("CPU_MAX", (int)Double.parseDouble(value));
            else if(isRAM) record1.put("RAM_MAX", (int)Double.parseDouble(value));
            else if(isDisk) record1.put("DISK_MAX", (int)Double.parseDouble(value));
        }else{
            if(isCPU) record1.put("CPU", Double.parseDouble(value));
            else if(isRAM) record1.put("RAM", Double.parseDouble(value));
            else if(isDisk) record1.put("DISK", Double.parseDouble(value));
            else if(isCount) record1.put("count", (int)Double.parseDouble(value));
        }
        records.put(key, record1);
    }

    private static void wordCount(String fileName) {
        schema = parseSchema();
        SparkConf sparkConf = new SparkConf().setMaster("local").setAppName("Speed Layer");
        JavaSparkContext sparkContext = new JavaSparkContext(sparkConf);
        JavaRDD<String> messages = sparkContext.textFile(fileName);

        JavaRDD<String> objects = messages.map(SpeedLayer::tokenize).flatMap(x -> Arrays.stream(x).iterator());
        JavaPairRDD<String, String> rdd = objects.map(x -> parser.fromJson(x, Info.class)).flatMapToPair(SpeedLayer::extractData);
        JavaPairRDD<String, String> reducedRDD = rdd.filter(x -> !x._1.contains("MAX")).reduceByKey(SpeedLayer::reduce);
        JavaPairRDD<String, String> reducedMaxRDD = rdd.filter(x -> x._1.contains("MAX")).reduceByKey(SpeedLayer::reduceMax);

        reducedRDD.collect().forEach(SpeedLayer::addRecord);
        reducedMaxRDD.collect().forEach(SpeedLayer::addRecord);

        List<GenericData.Record> recordList = new ArrayList<>(records.values());

        writeToParquetFile(recordList, schema);

        //reducedRDD.collect().forEach(tuple -> System.out.println(tuple._1 + " " + tuple._2));
        //reducedMaxRDD.collect().forEach(tuple -> System.out.println(tuple._1 + " " + tuple._2));
    }

    public static void main(String[] args) {
/*
        if (args.length == 0) {
            System.out.println("No files provided.");
            System.exit(0);
        }
*/
        wordCount("/home/moaz/health.txt");
    }

    @Override
    public void wakeup() {

    }
}
