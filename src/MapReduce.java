import java.io.IOException;
import java.util.Arrays;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.shaded.com.google.gson.Gson;

public class MapReduce {

    public static class TokenizerMapper extends Mapper<Object, Text, Text, DoubleWritable>{

        private final static DoubleWritable one = new DoubleWritable(1);
        private Text word = new Text();
        Gson parser = new Gson();

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String []messages =value.toString().split("}}");
            for (String message : messages) {
                message=message.concat("}}");
                Info currentInfo = parser.fromJson(message, Info.class);
                /*
                System.out.println("====>"+message);
                System.out.println("Service: "+currentInfo.serviceName);
                System.out.println("CPU: "+currentInfo.CPU);
                System.out.println("Total Disk: "+currentInfo.Disk.Total);
                System.out.println("Free Disk: "+currentInfo.Disk.Free);
                System.out.println("Total RAM: "+currentInfo.RAM.Total);
                System.out.println("Free RAM: "+currentInfo.RAM.Free);
                */
                //CPU utilization
                word.set(currentInfo.serviceName+"_CPU");
                context.write(word, new DoubleWritable(currentInfo.Disk.Total));
                //Disk utilization
                word.set(currentInfo.serviceName+"_Disk");
                context.write(word, new DoubleWritable(currentInfo.Disk.Free/currentInfo.Disk.Total));
                // RAM utilization
                word.set(currentInfo.serviceName+"_RAM");
                context.write(word, new DoubleWritable(currentInfo.RAM.Free/currentInfo.RAM.Total));
                // count of health messages for each service
                word.set(currentInfo.serviceName);
                context.write(word, one);
            }
        }
    }

    public static class IntSumReducer extends Reducer<Text,DoubleWritable,Text,DoubleWritable> {
        private DoubleWritable result = new DoubleWritable();

        public void reduce(Text key, Iterable<DoubleWritable> values, Context context ) throws IOException, InterruptedException {
            Double sum = 0.0;
            for (DoubleWritable val : values) {
                sum += val.get();
            }
            result.set(sum);
            //System.out.println("---------->" + key.toString());
            context.write(key, result);
        }
    }

    public static void main(String[] args) throws Exception {

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
        FileInputFormat.addInputPath(job, new Path("/health_data"));
        FileOutputFormat.setOutputPath(job, new Path("/output"));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}