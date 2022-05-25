package com.university.bigdata.milestone3;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;

import java.io.BufferedInputStream;
import java.io.FileInputStream;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;

public class FileWriteToHDFS {

    public static void main(String[] args) throws Exception {

//Source file in the local file system
        String localSrc = args[0];
//Destination file in HDFS
        String dst = args[1];

//Input stream for the file in local file system to be written to HDFS
        InputStream in = new BufferedInputStream(new FileInputStream(localSrc));

//Get configuration of Hadoop system
        Configuration conf = new Configuration();
        conf.set("fs.hdfs.impl", "org.apache.hadoop.hdfs.DistributedFileSystem");
        Path coreSite = new Path("/usr/local/hadoop/etc/hadoop/core-site.xml");
        Path hdfsSite = new Path("/usr/local/hadoop/etc/hadoop/hdfs-site.xml");
        conf.addResource(coreSite);
        conf.addResource(hdfsSite);
        System.out.println("Connecting to -- " + conf.get("fs.defaultFS"));

//Destination file in HDFS
        FileSystem fs = FileSystem.get(URI.create(dst), conf);
        System.out.println(fs.getConf().get("dfs.support.append"));
        System.out.println(dst);
        OutputStream out = fs.create(new Path(dst));

//Copy file from local to HDFS
        IOUtils.copyBytes(in, out, 32 * 1024, true);

        System.out.println(dst + " copied to HDFS");

    }
}