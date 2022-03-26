import com.sun.management.OperatingSystemMXBean;
import java.lang.management.ManagementFactory;

public class Usage {

    public static void main(String args[]){
        printUsage();
    }

    public static void printUsage() {
        OperatingSystemMXBean osBean = ManagementFactory.getPlatformMXBean(
                OperatingSystemMXBean.class);

// What % load the overall system is at, from 0.0-1.0
        System.out.println(osBean.getSystemCpuLoad());
    }

}
