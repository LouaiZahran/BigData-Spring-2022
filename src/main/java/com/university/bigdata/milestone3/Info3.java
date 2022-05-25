package com.university.bigdata.milestone3;

public class Info3 {
    public int service;
    public double CPU;
    public double RAM;
    public double Disk;
    public int count;
    public double maxCpu;
    public double maxRam;
    public double maxDisk;

    public Info3(int service,
            double CPU,
            double RAM,
            double Disk,
            int count,
            double maxCpu,
            double maxRam,
            double maxDisk) {
        this.service = service;
        this.CPU = CPU;
        this.RAM = RAM;
        this.Disk = Disk;
        this.count = count;
        this.maxCpu = maxCpu;
        this.maxRam = maxRam;
        this.maxDisk = maxDisk;
    }

    public Info3(int service) {
        this.service = service;
        this.CPU = 0;
        this.RAM = 0;
        this.Disk = 0;
        this.count = 0;
        this.maxCpu = 0;
        this.maxRam = 0;
        this.maxDisk = 0;
    }

    public void update(Info3 other) {
        this.count = other.count + this.count;
        this.CPU = (this.count != 0) ? (other.CPU * other.count + this.CPU * this.count) / (other.count + this.count)
                : 0;
        this.RAM = (this.count != 0) ? (other.RAM * other.count + this.RAM * this.count) / (other.count + this.count)
                : 0;
        this.Disk = (this.count != 0) ? (other.Disk * other.count + this.Disk * this.count) / (other.count + this.count)
                : 0;
        this.maxCpu = Math.max(other.maxCpu, this.maxCpu);
        this.maxRam = Math.max(other.maxRam, this.maxRam);
        this.maxDisk = Math.max(other.maxDisk, this.maxDisk);
    }

}
