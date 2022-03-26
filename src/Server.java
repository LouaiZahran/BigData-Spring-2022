import org.apache.hadoop.util.Time;

import java.io.FileWriter;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.util.ArrayList;

public class Server {
    public static void main(String[] args) throws Exception
    {
        //Create a socket to listen at port 3500
        DatagramSocket socket = new DatagramSocket(3500);

        //Buffer to hold the 1024 messages
        ArrayList<String> buffer =new ArrayList();

        //message receiver
        byte[] receive = new byte[10000];

        DatagramPacket packet = null;
        while (true){

            packet = new DatagramPacket(receive, receive.length);

            socket.receive(packet);

            buffer.add(toString(receive)) ;
            System.out.println("Client:-" + toString(receive));

            if(buffer.size() == 10){
                System.out.println("Writing to HDFS..");

                String fileName = "1000"+ ".log";
                FileWriter writer = new FileWriter(fileName);
                for(int i=0; i<10; i++)
                    writer.write(buffer.get(i));
                writer.close();

                String arg[] = new String[2];
                arg[0] = fileName;
                arg[1] = "/" + fileName;
                FileWriteToHDFS.main(arg);
            }
            // Clear the buffer after every message.
            receive = new byte[65535];
        }
    }

    public static void HDFSWrite(ArrayList<String> buffer){
        System.out.printf("Write to HDFS");
        buffer.clear();
    }
    // A utility method to convert the byte array
    // data into a string representation.
    public static String toString(byte[] a)
    {
        if (a == null)
            return null;
        StringBuilder ret = new StringBuilder();
        int i = 0;
        while (a[i] != 0)
        {
            ret.append((char) a[i]);
            i++;
        }
        return ret.toString();
    }
}
