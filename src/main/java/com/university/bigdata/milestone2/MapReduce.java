package com.university.bigdata.milestone2;
import com.university.bigdata.milestone3.Obs;
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
import java.io.File;
import java.util.HashMap;
import java.util.Map;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.parquet.avro.AvroParquetOutputFormat;
import org.apache.parquet.example.data.Group;

public class MapReduce extends Configured implements Tool, Obs {
    int Day=0;
    static Map<String,GenericRecord> mp=new HashMap<>();

    /// Schema
    private static Schema schema ;
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

    @Override
    public void wakeup() {
        String []args=new String[1];
        args[0]="/home/moaz/Downloads/health_data/health_"+Day+".json";
        try {
            MapReduce.main(args);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static class TokenizerMapper extends Mapper<Object, Text, Text, DoubleWritable>{

        private final static DoubleWritable one = new DoubleWritable(1);
        private Text word = new Text();
        Gson parser = new Gson();

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String []messages =value.toString().split("}}");
            for (String message : messages) {
                message=message.concat("}}");
                Info currentInfo = parser.fromJson(message, Info.class);

                long time=(currentInfo.Timestamp - 1647938697)/60;
                //CPU utilization
                word.set(currentInfo.serviceName+"_"+time+"_CPU");
                context.write(word, new DoubleWritable(currentInfo.CPU));
                //CPU MAX utilization
                word.set(currentInfo.serviceName+"_"+time+"_CPU_MAX");
                context.write(word, new DoubleWritable(currentInfo.CPU));

                //Disk utilization
                word.set(currentInfo.serviceName+"_"+time+"_DISK");
                context.write(word, new DoubleWritable(currentInfo.Disk.Free/currentInfo.Disk.Total));
                //Disk MAX utilization
                word.set(currentInfo.serviceName+"_"+time+"_DISK_MAX");
                context.write(word, new DoubleWritable(currentInfo.Disk.Free/currentInfo.Disk.Total));

                // RAM utilization
                word.set(currentInfo.serviceName+"_"+time+"_RAM");
                context.write(word, new DoubleWritable(currentInfo.RAM.Free/currentInfo.RAM.Total));
                // RAM MAX utilization
                word.set(currentInfo.serviceName+"_"+time+"_RAM_MAX");
                context.write(word, new DoubleWritable(currentInfo.RAM.Free/currentInfo.RAM.Total));

                // count of health messages for each service
                word.set(currentInfo.serviceName+"_"+time);
                context.write(word, one);
            }
        }
    }

    public static class IntSumReducer extends Reducer<Text,DoubleWritable,Void,GenericRecord> {

        public void reduce(Text key, Iterable<DoubleWritable> values, Context context ) throws IOException, InterruptedException {
            double ans = 0.0;
            if(!key.toString().contains("MAX"))
                for (DoubleWritable val : values) {
                    ans += val.get();
                }
            else
                for (DoubleWritable val : values) {
                    ans = max(val.get(),ans);
                }

            String[] keys=key.toString().split("_");
            String KEY= keys[0]+"_"+keys[1] ;


            mp.putIfAbsent(KEY,new GenericData.Record(schema));

            if(keys.length == 2){
                mp.get(KEY).put("count", ans);
                mp.get(KEY).put("service", keys[0].charAt(8)-'0');
                mp.get(KEY).put("time", Integer.parseInt(keys[1]));
            }
            else if(keys.length == 3){
                mp.get(KEY).put(keys[2], ans);
            }
            else{
                mp.get(KEY).put(keys[2]+"_"+keys[3], ans);
            }
            if(!mp.get(KEY).toString().contains("null")){
                context.write(null, mp.get(KEY));
            }
            //System.out.println("---------->" + key.toString());
            //System.out.println("============" + mp.get(KEY).toString());

        }
    }

    public static void main(String[] args) throws Exception {
        schema = parseSchema();
        int exitFlag = ToolRunner.run(new MapReduce(), args);
        System.exit(exitFlag);
    }

    @Override
    public int run(String[] args) throws Exception {
        Configuration conf = new Configuration();
/*
        conf.set("fs.hdfs.impl", "org.apache.hadoop.hdfs.DistributedFileSystem");
        Path coreSite = new Path("/usr/local/hadoop/etc/hadoop/core-site.xml");
        Path hdfsSite = new Path("/usr/local/hadoop/etc/hadoop/hdfs-site.xml");
        conf.addResource(coreSite);
        conf.addResource(hdfsSite);
*/
        Job job = Job.getInstance(conf, "word count");
        job.setJarByClass(MapReduce.class);
        job.setMapperClass(TokenizerMapper.class);
        //job.setCombinerClass(IntSumReducer.class);
        job.setReducerClass(IntSumReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(DoubleWritable.class);



        job.setOutputKeyClass(Void.class);
        job.setOutputValueClass(Group.class);

        job.setOutputFormatClass(AvroParquetOutputFormat.class);

        // setting schema to be used
        AvroParquetOutputFormat.setSchema(job, schema);

        String filePath;
        if(args.length !=1){
            System.out.println("Wrong file input format");
            System.exit(0);
        }

        FileInputFormat.addInputPath(job, new Path(args[0]));
        Path outputPath = new Path("batch_view/days");
        FileSystem fileSystem = FileSystem.get(conf);
        if(fileSystem.exists(outputPath))
            fileSystem.delete(outputPath);
        FileOutputFormat.setOutputPath(job, outputPath);
        job.waitForCompletion(true);
        return job.waitForCompletion(true) ? 0 : 1;
    }
}