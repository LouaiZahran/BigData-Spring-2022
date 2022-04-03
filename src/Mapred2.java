import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.shaded.com.google.gson.Gson;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;
import java.util.StringTokenizer;

public class Mapred2 extends Configured implements Tool {

    public static class Data{
        Text serviceName;
        Usage usage;

        public Data(Info info){
            this.serviceName = new Text(info.serviceName);
            this.usage = new Usage(info.CPU,
                    info.Disk.Free / info.Disk.Total * 100,
                    info.RAM.Free / info.RAM.Total * 100);
        }

        public Data(Text serviceName, Usage usage){
            this.serviceName = serviceName;
            this.usage = usage;
        }

        public static Data get(Text serviceName, Usage usage){
            return new Data(serviceName, usage);
        }
    }

    public static class Usage{
        double CPU;
        double Disk;
        double RAM;

        public Usage(double CPU, double Disk, double RAM) {
            this.CPU = CPU;
            this.Disk = Disk;
            this.RAM = RAM;
        }

        public static Usage get(double CPU, double Disk, double RAM) {
            return new Usage(CPU, Disk, RAM);
        }

    }

    public static class Map2 extends Mapper<Object, Text, Text, Usage>{

        Gson parser = new Gson();

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            StringTokenizer itr = new StringTokenizer(value.toString());
            int count = 0;
            while(itr.hasMoreTokens()){
                if(count == 10)
                    break;
                count++;
                String token = itr.nextToken("}}").concat("}}");
                Info currentInfo = parser.fromJson(token, Info.class);
                System.out.println("Service: "+currentInfo.serviceName);
                System.out.println("CPU: "+currentInfo.CPU);
                System.out.println("Disk: "+currentInfo.Disk);
                System.out.println("RAM: "+currentInfo.RAM);
                Data currentData = new Data(currentInfo);
                context.write(currentData.serviceName, currentData.usage);
            }
        }
    }

    public static class Reduce extends Reducer<Text, Usage, Text, Data> {

        public void reduce(Text key, Iterable<Usage> values, Context context)
                throws IOException, InterruptedException {

            int count = 0;
            double CPUSum = 0;
            double RAMSum = 0;
            double DiskSum = 0;
            for (Usage usage : values) {
                count++;
                CPUSum+=usage.CPU;
                RAMSum+=usage.RAM;
                DiskSum+=usage.Disk;
            }
            double CPUAvg = CPUSum/count;
            double RAMAvg = RAMSum/count;
            double DiskAvg = DiskSum/count;

            Usage reducedUsage = Usage.get(CPUAvg, DiskAvg, RAMAvg);
            Data reducedData = Data.get(key, reducedUsage);
            context.write(key, reducedData);
        }
    }

    public static void main(String[] args) throws Exception {
        ToolRunner.run(new Configuration(), new Mapred2(), args);
    }

    public int run(String[] args) throws Exception {
        Configuration conf = new Configuration();
        conf.set("fs.hdfs.impl", "org.apache.hadoop.hdfs.DistributedFileSystem");
        Path coreSite = new Path("/usr/local/hadoop/etc/hadoop/core-site.xml");
        Path hdfsSite = new Path("/usr/local/hadoop/etc/hadoop/hdfs-site.xml");
        conf.addResource(coreSite);
        conf.addResource(hdfsSite);

        Job job = Job.getInstance(conf, "MapRed");
        job.setJarByClass(Mapred2.class);
        job.setMapperClass(Map2.class);
        job.setCombinerClass(Reduce.class);
        job.setReducerClass(Reduce.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Data.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        Path outputPath = new Path(args[1]);
        FileSystem fileSystem = FileSystem.get(conf);
        if(fileSystem.exists(outputPath))
            fileSystem.delete(outputPath);
        FileOutputFormat.setOutputPath(job, outputPath);
        boolean ret = job.waitForCompletion(true);
        return (ret? 0:1);
    }
}
