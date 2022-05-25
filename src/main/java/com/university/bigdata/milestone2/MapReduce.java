package com.university.bigdata.milestone2;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.shaded.com.google.gson.Gson;

import java.io.IOException;

import static java.lang.Math.max;

public class MapReduce {

    static long start, end;

    public static class TokenizerMapper extends Mapper<Object, Text, Text, DoubleWritable>{

        private final static DoubleWritable one = new DoubleWritable(1);
        private Text word = new Text();
        Gson parser = new Gson();

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String []messages =value.toString().split("}}");
            for (String message : messages) {
                message=message.concat("}}");
                Info currentInfo = parser.fromJson(message, Info.class);

                //currentInfo.Timestamp

                if(currentInfo.Timestamp < start || currentInfo.Timestamp > end)
                    continue;
                long time=(currentInfo.Timestamp - 1647938697)/60;
                //CPU utilization
                word.set(currentInfo.serviceName+"_CPU_"+time);
                context.write(word, new DoubleWritable(currentInfo.CPU));

                word.set(currentInfo.serviceName+"_CPU_MAX_"+time);
                context.write(word, new DoubleWritable(currentInfo.CPU));

                //Disk utilization
                word.set(currentInfo.serviceName+"_DISK_"+time);
                context.write(word, new DoubleWritable(currentInfo.Disk.Free/currentInfo.Disk.Total));
                // RAM utilization
                word.set(currentInfo.serviceName+"_RAM_"+time);
                context.write(word, new DoubleWritable(currentInfo.RAM.Free/currentInfo.RAM.Total));
                // count of health messages for each service
                word.set(currentInfo.serviceName+"_"+time);
                context.write(word, one);
            }
        }
    }

    public static class IntSumReducer extends Reducer<Text,DoubleWritable,Text,DoubleWritable> {
        private DoubleWritable result = new DoubleWritable();

        public void reduce(Text key, Iterable<DoubleWritable> values, Context context ) throws IOException, InterruptedException {
            Double ans = 0.0;
            if(key.toString().length() <15 || key.toString().charAt(14)!= 'M')
                for (DoubleWritable val : values) {
                    ans += val.get();
                }
            else
                for (DoubleWritable val : values) {
                    ans = max(val.get(),ans);
                }
            result.set(ans);

            System.out.println("---------->" + key.toString());

            context.write(key, result);
        }
    }

    public static void main(String[] args) throws Exception {

        System.out.println(args[0]);
        start = Long.parseLong(args[0]);
        end = Long.parseLong(args[1]);
        Configuration conf = new Configuration();
        
        conf.set("fs.hdfs.impl", "org.apache.hadoop.hdfs.DistributedFileSystem");
        Path coreSite = new Path("/usr/local/hadoop/etc/hadoop/core-site.xml");
        Path hdfsSite = new Path("/usr/local/hadoop/etc/hadoop/hdfs-site.xml");
        conf.addResource(coreSite);
        conf.addResource(hdfsSite);

        Job job = Job.getInstance(conf, "word count");
        job.setJarByClass(MapReduce.class);
        job.setMapperClass(TokenizerMapper.class);
        job.setCombinerClass(IntSumReducer.class);
        job.setReducerClass(IntSumReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(DoubleWritable.class);
        FileInputFormat.addInputPath(job, new Path("/health_data/health_0.json"));
        Path outputPath = new Path("/health_out");
        FileSystem fileSystem = FileSystem.get(conf);
        if(fileSystem.exists(outputPath))
            fileSystem.delete(outputPath);
        FileOutputFormat.setOutputPath(job, outputPath);
        job.waitForCompletion(true);
    }
}