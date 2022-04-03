import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.shaded.com.google.gson.Gson;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import com.google.common.base.Charsets;

public class Mapred extends Configured implements Tool {

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

    private double mean = 0;

    private final static Text COUNT = new Text("count");
    private final static Text LENGTH = new Text("length");
    private final static LongWritable ONE = new LongWritable(1);

    public static class Map extends Mapper<Object, Text, Text, Usage>{

        Gson parser = new Gson();

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String str = value.toString();
            String currentString = "";
            int length = str.length();
            int count = 0;
            for(int i=0; i<length; i++){
                if(count == 10)
                    break;
                currentString = currentString.concat(String.valueOf(str.charAt(i)));
                if(currentString.endsWith("}}")){
                    count++;
                    System.out.println(currentString);
                    Info currentInfo = parser.fromJson(currentString, Info.class);
                    System.out.println("Service: "+currentInfo.serviceName);
                    System.out.println("CPU: "+currentInfo.CPU);
                    System.out.println("Disk: "+currentInfo.Disk.Total + " " + currentInfo.Disk.Free);
                    System.out.println("RAM: "+currentInfo.RAM.Total + " " + currentInfo.RAM.Free);
                    Data currentData = new Data(currentInfo);
                    context.write(currentData.serviceName, currentData.usage);
                    currentString = "";
                }
            }
        }
    }

    public static class WordMeanMapper extends
            Mapper<Object, Text, Text, LongWritable> {

        private LongWritable wordLen = new LongWritable();

        public void map(Object key, Text value, Context context)
                throws IOException, InterruptedException {
            StringTokenizer itr = new StringTokenizer(value.toString());
            while (itr.hasMoreTokens()) {
                String string = itr.nextToken();
                this.wordLen.set(string.length());
                context.write(LENGTH, this.wordLen);
                context.write(COUNT, ONE);
            }
        }
    }

    public static class WordMeanReducer extends
            Reducer<Text, LongWritable, Text, LongWritable> {

        private LongWritable sum = new LongWritable();
        public void reduce(Text key, Iterable<LongWritable> values, Context context)
                throws IOException, InterruptedException {

            int theSum = 0;
            for (LongWritable val : values) {
                theSum += val.get();
            }
            sum.set(theSum);
            context.write(key, sum);
        }
    }
    private double readAndCalcMean(Path path, Configuration conf)
            throws IOException {
        FileSystem fs = FileSystem.get(conf);
        Path file = new Path(path, "part-r-00000");

        if (!fs.exists(file))
            throw new IOException("Output not found!");

        BufferedReader br = null;

        // average = total sum / number of elements;
        try {
            br = new BufferedReader(new InputStreamReader(fs.open(file), Charsets.UTF_8));

            long count = 0;
            long length = 0;

            String line;
            while ((line = br.readLine()) != null) {
                StringTokenizer st = new StringTokenizer(line);

                // grab type
                String type = st.nextToken();

                // differentiate
                if (type.equals(COUNT.toString())) {
                    String countLit = st.nextToken();
                    count = Long.parseLong(countLit);
                } else if (type.equals(LENGTH.toString())) {
                    String lengthLit = st.nextToken();
                    length = Long.parseLong(lengthLit);
                }
            }

            double theMean = (((double) length) / ((double) count));
            System.out.println("The mean is: " + theMean);
            return theMean;
        } finally {
            if (br != null) {
                br.close();
            }
        }
    }

    public static void main(String[] args) throws Exception {
        ToolRunner.run(new Configuration(), new Mapred(), args);
    }

    @Override
    public int run(String[] args) throws Exception {
        if (args.length != 2) {
            System.err.println("Usage: wordmean <in> <out>");
            return 0;
        }

        Configuration conf = new Configuration();
        conf.set("fs.hdfs.impl", "org.apache.hadoop.hdfs.DistributedFileSystem");
        Path coreSite = new Path("/usr/local/hadoop/etc/hadoop/core-site.xml");
        Path hdfsSite = new Path("/usr/local/hadoop/etc/hadoop/hdfs-site.xml");
        conf.addResource(coreSite);
        conf.addResource(hdfsSite);

        Job job = Job.getInstance(conf, "word mean");
        job.setJarByClass(Mapred.class);
        job.setMapperClass(WordMeanMapper.class);
        job.setCombinerClass(WordMeanReducer.class);
        job.setReducerClass(WordMeanReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(LongWritable.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        Path outputpath = new Path(args[1]);
        FileSystem fileSystem = FileSystem.get(conf);
        if(fileSystem.exists(outputpath))
            fileSystem.delete(outputpath);
        FileOutputFormat.setOutputPath(job, outputpath);
        boolean result = job.waitForCompletion(true);
        //mean = readAndCalcMean(outputpath, conf);

        return (result ? 0 : 1);
    }

    /**
     * Only valuable after run() called.
     *
     * @return Returns the mean value.
     */
    public double getMean() {
        return mean;
    }
}