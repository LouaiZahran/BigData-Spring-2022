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
    public long maxCPUtime;
    public long maxRAMtime;
    public long maxDISKtime;

    public Info3(int service,
            double CPU,
            double RAM,
            double Disk,
            int count,
            long maxCPUtime,
            long maxRAMtime,
            long maxDISKtime,
            double maxCpu,
            double maxRam,
            double maxDisk) {
        this.service = service;
        this.CPU = CPU;
        this.RAM = RAM;
        this.Disk = Disk;
        this.count = count;
        this.maxCPUtime = maxCPUtime;
        this.maxRAMtime = maxRAMtime;
        this.maxDISKtime = maxDISKtime;
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
        this.maxCPUtime = 0;
        this.maxRAMtime = 0;
        this.maxDISKtime = 0;

    }

    public void update(Info3 other) {
        this.CPU +=other.CPU ;
        this.RAM +=other.RAM ;
        this.Disk +=other.Disk ;
        if(this.maxCpu < other.maxCpu){
            this.maxCPUtime = other.maxCPUtime;
        }
        if(this.maxRam < other.maxRam){
            this.maxRAMtime = other.maxRAMtime;
        }
        if(this.maxDisk < other.maxDisk){
            this.maxDISKtime = other.maxDISKtime;
        }
        this.maxCpu = Math.max(other.maxCpu, this.maxCpu);
        this.maxRam = Math.max(other.maxRam, this.maxRam);
        this.maxDisk = Math.max(other.maxDisk, this.maxDisk);
        this.count = other.count + this.count;

    }

}
