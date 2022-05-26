package com.university.bigdata.milestone3;

import java.io.FileWriter;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;

public class SpeedClient {
    public static void main(String[] args) throws Exception
    {
        boolean isBatchLayer = false;

        //Create a socket to listen at port 3505
        DatagramSocket socket = new DatagramSocket(3505);

        //Buffer to hold the 1024 messages
        ArrayList<String> buffer =new ArrayList();

        //message receiver
        byte[] receive = new byte[10000];

        DatagramPacket packet = null;
        int counter = 0;
        while (true){

            packet = new DatagramPacket(receive, receive.length);

            socket.receive(packet);

            buffer.add(toString(receive)) ;
            System.out.println("Client:-" + toString(receive));

            if(buffer.size() == 10){

                String fileName = counter + ".log";
                counter++;
                FileWriter writer = new FileWriter("SpeedLayer/messages/" + fileName);
                for(int i=0; i<10; i++)
                    writer.write(buffer.get(i));
                writer.close();

                String arg[] = new String[2];
                arg[0] = fileName;
                arg[1] = "/" + fileName;
                if(isBatchLayer){
                    System.out.println("Writing to HDFS..");
                    FileWriteToHDFS.main(arg);
                }
            }
            // Clear the buffer after every message.
            receive = new byte[65535];
        }
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