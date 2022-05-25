package com.university.bigdata.milestone3;

import com.google.gson.Gson;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

public class SpeedLayer implements Obs{

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

    private static void wordCount(String fileName) {
        SparkConf sparkConf = new SparkConf().setMaster("local").setAppName("Speed Layer");
        JavaSparkContext sparkContext = new JavaSparkContext(sparkConf);
        JavaRDD<String> objs = sparkContext.textFile(fileName);

        objs.collect();
        JavaRDD<String> objs2 = objs.map(SpeedLayer::tokenize).flatMap(x -> Arrays.stream(x).iterator());
        JavaPairRDD<String, String> rdd = objs2.map(x -> parser.fromJson(x, Info.class)).flatMapToPair(SpeedLayer::extractData);
        JavaPairRDD<String, String> reducedRDD = rdd.filter(x -> x._1.contains("MAX")).reduceByKey(SpeedLayer::reduce);
        JavaPairRDD<String, String> reducedMaxRDD = rdd.filter(x -> x._1.contains("MAX")).reduceByKey(SpeedLayer::reduceMax);

        reducedRDD.collect().forEach(tuple -> System.out.println(tuple._1 + " " + tuple._2));
        reducedMaxRDD.collect().forEach(tuple -> System.out.println(tuple._1 + " " + tuple._2));

    }

    public static void main(String[] args) {

        if (args.length == 0) {
            System.out.println("No files provided.");
            System.exit(0);
        }

        wordCount(args[0]);
    }

    @Override
    public void wakeup() {

    }
}
