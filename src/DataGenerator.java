import java.sql.Time;

public class DataGenerator {
    public static Info generate(String serviceName, double RAMTotal, double DiskTotal){
        long timestamp = 1000;
        double CPU = Math.random();

        RAMInfo RAM = new RAMInfo();
        RAM.total = RAMTotal;
        RAM.free = Math.random() * RAM.total;

        DiskInfo Disk = new DiskInfo();
        Disk.total = DiskTotal;
        Disk.free = Math.random() * Disk.total;

        return new Info(serviceName, timestamp, CPU, RAM, Disk);
    }
}
