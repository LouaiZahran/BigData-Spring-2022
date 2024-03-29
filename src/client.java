import java.io.IOException;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetAddress;
import com.google.gson.Gson;

public class client {
    public static void main(String args[]) throws IOException, InterruptedException {
        String serviceName = "Louai";
        double RAM = 8;
        double Disk = 1024;
        Gson parser = new Gson();
        DatagramSocket ds = new DatagramSocket();

        InetAddress ip = InetAddress.getByName("192.168.43.149");
        byte buf[] = null;

        Integer i=0;
        while (true)
        {
            Thread.sleep(10);
            Info msg = DataGenerator.generate(serviceName, RAM, Disk);
            String inp = parser.toJson(msg);

            buf = inp.getBytes();

            DatagramPacket DpSend =
                        new DatagramPacket(buf, buf.length, ip, 3500);

            ds.send(DpSend);
        }
    }
}
