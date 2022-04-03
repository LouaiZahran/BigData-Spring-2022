class RAMInfo{
    public double Total;
    public double Free;
}

class DiskInfo{
    public double Total;
    public double Free;
}

public class Info {
    public String serviceName;
    public long Timestamp;
    public double CPU;
    public RAMInfo RAM;
    public DiskInfo Disk;

    public Info(String serviceName, long timestamp, double CPU, RAMInfo RAM, DiskInfo disk) {
        this.serviceName = serviceName;
        this.Timestamp = timestamp;
        this.CPU = CPU;
        this.RAM = RAM;
        this.Disk = disk;
    }

}
