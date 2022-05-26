package com.university.bigdata.milestone3;

import com.university.bigdata.milestone2.MapReduce;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.shaded.net.minidev.json.JSONObject;
import org.springframework.web.bind.annotation.CrossOrigin;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.io.InputStream;
import java.util.Scanner;

@RestController
@CrossOrigin()
public class Controller {

    @GetMapping("/analytics")
    String getAnalytics(@RequestParam String start, @RequestParam String end) throws Exception {
        System.out.println("API WORKS!");

        Info3[] queryResponse = Query.getQuery(Long.parseLong(start), Long.parseLong(end));

        String response = "";

        for(int i=1; i<=4; i++) {
            Info3 currentInfo = queryResponse[i];
            JSONObject obj = new JSONObject();
            obj.put("servicename", currentInfo.service);
            obj.put("cpu", currentInfo.CPU);
            obj.put("disk", currentInfo.Disk);
            obj.put("ram", currentInfo.RAM);
            obj.put("cpumax", currentInfo.maxCpu);
            obj.put("diskmax", currentInfo.maxDisk);
            obj.put("rammax", currentInfo.maxRam);
            obj.put("cpumaxtime", currentInfo.maxCPUtime);
            obj.put("diskmaxtime", currentInfo.maxDISKtime);
            obj.put("rammaxtime", currentInfo.maxRAMtime);
            response = response.concat(obj.toString());
            response = response.concat("\n");
        }

        System.out.println(response);
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
