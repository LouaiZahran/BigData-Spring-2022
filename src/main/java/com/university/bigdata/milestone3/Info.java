package com.university.bigdata.milestone3;

class RAM {
    public double Total;
    public double Free;
}

class Disk {
    public double Total;
    public double Free;
}

public class Info {
    public String serviceName;
    public long Timestamp;
    public double CPU;
    public RAM RAM;
    public Disk Disk;

    public Info(String serviceName, long timestamp, double CPU, RAM RAM, Disk disk) {
        this.serviceName = serviceName;
        this.Timestamp = timestamp;
        this.CPU = CPU;
        this.RAM = RAM;
        this.Disk = disk;
    }

    public void print(){
        System.out.println("Service Name: " + serviceName);
        System.out.println("Timestamp: " + Timestamp);
        System.out.println("CPU: " + CPU);
        System.out.println("RAM: " + RAM.Free);
        System.out.println("Disk: " + Disk.Free);
    }
}
