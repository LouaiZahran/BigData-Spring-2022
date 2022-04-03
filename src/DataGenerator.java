public class DataGenerator {
    public static Info generate(String serviceName, double RAMTotal, double DiskTotal){
        long timestamp = 1000;
        double CPU = Math.random();

        RAMInfo RAM = new RAMInfo();
        RAM.Total = RAMTotal;
        RAM.Free = Math.random() * RAM.Total;

        DiskInfo Disk = new DiskInfo();
        Disk.Total = DiskTotal;
        Disk.Free = Math.random() * Disk.Total;

        return new Info(serviceName, timestamp, CPU, RAM, Disk);
    }
}
