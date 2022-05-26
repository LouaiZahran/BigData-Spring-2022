package com.university.bigdata.milestone3;

        import java.io.File;
        import java.io.FileReader;
        import java.io.IOException;
        import java.net.DatagramPacket;
        import java.net.DatagramSocket;
        import java.net.InetAddress;
        import java.util.ArrayList;
        import java.util.Arrays;
        import java.util.List;

        import com.google.gson.Gson;

public class Server {
    public static void main(String args[]) throws IOException, InterruptedException {
        String serviceName = "Louai";
        double RAM = 8;
        double Disk = 1024;
        Gson parser = new Gson();
        DatagramSocket ds = new DatagramSocket();

        int currentHealthDataFile = 0;
        String path = "health_";
        File currentFile = new File(path + String.valueOf(currentHealthDataFile) + ".json");
        FileReader fileReader = new FileReader(currentFile);

        InetAddress speedIP = InetAddress.getLocalHost();
        InetAddress batchIP = InetAddress.getLocalHost();
        List<Byte> buf = new ArrayList<>();

        int currentChar = 0, prevChar = 0, idx = 0;
        while (true)
        {
            Thread.sleep(10);

            while(currentChar != -1){
                currentChar = fileReader.read();
                buf.add((byte)currentChar);
                if((char) currentChar == '}' && (char) prevChar == '}') { //End of message
                    byte[] buffer = new byte[buf.size()];
                    for(int i=0; i<buffer.length; i++) {
                        buffer[i] = buf.get(i);
                        System.out.print((char)buffer[i]);
                    }
                    DatagramPacket speedPacket = new DatagramPacket(buffer, buffer.length, speedIP, 3500);
                    DatagramPacket batchPacket = new DatagramPacket(buffer, buffer.length, batchIP, 3505);
                    ds.send(speedPacket);
                    ds.send(batchPacket);
                }
                prevChar = currentChar;
            }

            currentHealthDataFile++;
            currentFile = new File(path + String.valueOf(currentHealthDataFile) + ".json");
            if(!currentFile.exists())
                break;
            fileReader = new FileReader(currentFile);
        }
    }
}
