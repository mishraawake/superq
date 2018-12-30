package org.apache.superq.network;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.SocketException;
import java.nio.ByteBuffer;

public class Client {

  public static void main(String[] args, int c) throws SocketException, IOException {

    String host = args.length > 0 ? args[0]: "127.0.0.1";
    int numberOfThread = args.length > 1 ? Integer.valueOf(args[1]): 1;
    int numberOfEntries = args.length > 2 ? Integer.valueOf(args[2]): 1;

    System.out.println("Runninng for host = "+host+" threads = "+numberOfThread+" numberOfEntries = "+numberOfEntries);
    for (int thread = 0; thread < numberOfThread; thread++) {
      Socket socket = getConsumer(numberOfEntries, host);
      getProducer(numberOfEntries, socket);
    }
  }


  private static Socket getProducer(int times, Socket socket) throws IOException {

     Thread t = new Thread(new Runnable() {
      @Override
      public void run() {
        try {

          long stime = System.currentTimeMillis();
          for (int i = 0; i < times; i++) {
           // stime = System.currentTimeMillis();
            StringBuilder messageb = new StringBuilder();
            messageb.append("Sending message!");
          /*  for (int j = 0; j < 1000 ; j++) {
              messageb.append("Sending message!");
            } */
            String message = messageb.toString();
            ByteBuffer bf = ByteBuffer.allocate(6 + message.length());
            bf.putInt(message.length());
            bf.putShort((short)0);
            bf.put(message.getBytes());
            bf.flip();
            socket.getOutputStream().write(bf.array());
          }
          System.out.println("Completed the client.." + (System.currentTimeMillis() - stime));
        } catch (IOException e){
          e.printStackTrace();
        }
      }
    });
     t.start();
    return socket;
  }


  private static Socket getConsumer(int times, String host) throws IOException {

    Socket socket = new Socket();
    //socket.setSoTimeout(10000);
    socket.setKeepAlive(true);
    socket.connect(new InetSocketAddress(host, 1234));
    Thread t = new Thread(new Runnable() {
      @Override
      public void run() {

          int count = 0;
          long stime = System.currentTimeMillis();
          while(true) {
            try {
              int remaining = 5;
              ByteBuffer bf = ByteBuffer.allocate(remaining);
              while (remaining > 0) {
                remaining -= socket.getInputStream().read(bf.array());
              }
              int destPort = Integer.valueOf(new String(bf.array()));
              ++count;
              if (destPort == socket.getPort()) {
                System.out.println("Response does not match " + destPort);
              }
              if (count % 100000 ==0) {
                System.out.println("Completed " + count + " time = " + (System.currentTimeMillis() - stime) + "");
                //socket.close();
                //break;
              }
              if (count == times) {
                System.out.println("Completed " + count + " time = " + (System.currentTimeMillis() - stime) + "");
                socket.close();
                break;
              }
            } catch (Exception e){
              e.printStackTrace();
              System.out.println(count);
            }
          }
      }
    });
    t.start();
    return socket;
  }

}
