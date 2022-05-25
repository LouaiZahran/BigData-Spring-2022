package com.university.bigdata.milestone3;

import com.google.gson.Gson;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

public class SpeedLayer implements Obs{

    private static String[] tokenize(String line){
        List<String> ret = new ArrayList<>();
        Gson parser = new Gson();
        String[] messages = line.toString().split("}}");
        for (String message : messages) {
            message = message.concat("}}");
            ret.add(message);
        }
        return ret.toArray(new String[0]);
    }

    private static String identity(String x){
        return x;
    }

    private static void wordCount(String fileName) {
        SparkConf sparkConf = new SparkConf().setMaster("local").setAppName("Speed Layer");
        JavaSparkContext sparkContext = new JavaSparkContext(sparkConf);
        JavaRDD<String> objs = sparkContext.textFile(fileName);

        objs.collect();
        JavaRDD<String> objs2 = objs.map(new Function<String, String[]>() {
            @Override
            public String[] call(String s) throws Exception {
                return tokenize(s);
            }
        }).flatMap(new FlatMapFunction<String[], String>() {
            @Override
            public Iterator<String> call(String[] stringStream) throws Exception {
                return Arrays.stream(stringStream).iterator();
            }
        });

        objs2.collect().forEach(System.out::println);

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
