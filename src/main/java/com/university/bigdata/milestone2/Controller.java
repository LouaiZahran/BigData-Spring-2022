package com.university.bigdata.milestone2;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.shaded.net.minidev.json.JSONObject;
import org.springframework.boot.json.GsonJsonParser;
import org.springframework.web.bind.annotation.*;

import java.io.InputStream;
import java.util.Scanner;

@RestController
@CrossOrigin()
public class Controller {

    /*
    const test= {
    servicename:'ms4',
    usage:{
        cpu: 70,
        disk: 60,
        ram:  60
    }
}
     */

    @GetMapping("/analytics")
    String getAnalytics(@RequestParam String start, @RequestParam String end) throws Exception {
        System.out.println("API WORKS!");
        String args[] = new String[2];
        args[0] = start;
        args[1] = end;
        MapReduce.main(args);
        Configuration conf = new Configuration();

        conf.set("fs.hdfs.impl", "org.apache.hadoop.hdfs.DistributedFileSystem");
        Path coreSite = new Path("/usr/local/hadoop/etc/hadoop/core-site.xml");
        Path hdfsSite = new Path("/usr/local/hadoop/etc/hadoop/hdfs-site.xml");
        conf.addResource(coreSite);
        conf.addResource(hdfsSite);

        FileSystem fileSystem = FileSystem.get(conf);
        InputStream inputStream = fileSystem.open(new Path("/health_out/part-r-00000"));
        String contents = toString(inputStream.readAllBytes());

        Scanner scanner = new Scanner(contents);
        String response = "";

        for(int i=0; i<4; i++) {
            if(!scanner.hasNext())
                break;
            String serviceName = scanner.next();
            scanner.next();

            scanner.next();
            double CPU = scanner.nextDouble();

            scanner.next();
            double disk = scanner.nextDouble();

            scanner.next();
            double ram = scanner.nextDouble();

            JSONObject obj = new JSONObject();
            obj.put("servicename", serviceName);
            obj.put("cpu", CPU);
            obj.put("disk", disk);
            obj.put("ram", ram);
            response = response.concat(obj.toString());
            response = response.concat("\n");
        }

        return response;
    }

    public static String toString(byte[] a)
    {
        if (a == null)
            return null;
        StringBuilder ret = new StringBuilder();
        int i = 0;
        while (i < a.length && a[i] != 0)
        {
            ret.append((char) a[i]);
            i++;
        }
        return ret.toString();
    }

}
