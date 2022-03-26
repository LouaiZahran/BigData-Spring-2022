import java.io.IOException;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.net.SocketException;
import java.util.ArrayList;

public class Server {
    public static void main(String[] args) throws IOException
    {
        //Create a socket to listen at port 3500
        DatagramSocket socket = new DatagramSocket(3500);

        //Buffer to hold the 1024 messages
        ArrayList<String> buffer =new ArrayList();

        //message receiver
        byte[] receive = new byte[10000];

        DatagramPacket packet = null;
        while (true){
            // Step 2 : create a DatgramPacket to receive the data.
            packet = new DatagramPacket(receive, receive.length);

            // Step 3 : revieve the data in byte buffer.
            socket.receive(packet);

            buffer.add(toString(receive)) ;
            System.out.println("Client:-" + toString(receive));

            if(buffer.size() == 100){
                HDFSWrite(buffer);
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
