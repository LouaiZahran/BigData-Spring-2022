import java.io.BufferedWriter;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.nio.charset.StandardCharsets;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;

public class writeToHDFS {
    public static void main(String[] args) throws IOException {
        Configuration conf = new Configuration();
        conf.set("fs.hdfs.impl", "org.apache.hadoop.hdfs.DistributedFileSystem");
        Path coreSite = new Path("/usr/local/hadoop/etc/hadoop/core-site.xml");
        Path hdfsSite = new Path("/usr/local/hadoop/etc/hadoop/hdfs-site.xml");
        conf.addResource(coreSite);
        conf.addResource(hdfsSite);
        FileSystem fileSystem = FileSystem.get(conf);
        String fileName = "read_write_hdfs_example.txt";
        Path hdfsWritePath = new Path("27_03_2022.log");

        fileSystem.setReplication(hdfsWritePath, (short) 1);
        if(!fileSystem.exists(hdfsWritePath)){
            fileSystem.create(hdfsWritePath);
        }
        FSDataOutputStream fsDataOutputStream = fileSystem.append(hdfsWritePath);
        BufferedWriter bufferedWriter = new BufferedWriter(new OutputStreamWriter(fsDataOutputStream, StandardCharsets.UTF_8));
        bufferedWriter.write("Java API to append data in HDFS file");
        bufferedWriter.newLine();
        bufferedWriter.close();
        fileSystem.close();

    }
}
