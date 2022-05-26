package com.university.bigdata.milestone3;

import com.google.gson.Gson;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.*;

public class SpeedLayer implements Obs{

    static Gson parser = new Gson();
    static Schema schema = ParquetWriter.parseSchema();
    static HashMap<String, GenericData.Record> records = new HashMap<>();

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
        ret.add(new Tuple2<>(name + "_" + time + "_Disk", String.valueOf(info.Disk.Free/info.Disk.Total)));
        ret.add(new Tuple2<>(name + "_" + time + "_Disk_MAX", String.valueOf(info.Disk.Free/info.Disk.Total)));
        ret.add(new Tuple2<>(name + "_" + time + "_RAM", String.valueOf(info.RAM.Free/info.RAM.Total)));
        ret.add(new Tuple2<>(name + "_" + time + "_RAM_MAX", String.valueOf(info.RAM.Free/info.RAM.Total)));
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
            if(isCPU) record1.put("CPU_MAX", Double.parseDouble(value));
            else if(isRAM) record1.put("RAM_MAX", Double.parseDouble(value));
            else if(isDisk) record1.put("DISK_MAX", Double.parseDouble(value));
        }else{
            if(isCPU) record1.put("CPU", Double.parseDouble(value));
            else if(isRAM) record1.put("RAM", Double.parseDouble(value));
            else if(isDisk) record1.put("DISK", Double.parseDouble(value));
            else if(isCount) record1.put("count", (int)Double.parseDouble(value));
        }
        records.put(key, record1);
    }

    private static void launchSpark(String fileName) {
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

        ParquetWriter.writeToParquetFile(recordList, schema);

        sparkContext.stop();
    }

    public static void main(String[] args) {

        if (args.length == 0) {
            System.out.println("No files provided.");
            System.exit(0);
        }

        launchSpark(args[0]);
    }

    @Override
    public void wakeup() {
        launchSpark("health_0.json");
    }
}
