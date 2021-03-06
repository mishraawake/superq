package org.apache.superq.network;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketAddress;
import java.nio.channels.SelectableChannel;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.superq.Broker;

public class BrokerServer {

  int port;
  String hostName = "127.0.0.1";
  public Map<String, ConnectionContext> allConnectionContext = new ConcurrentHashMap<>();
  Selector acceptingSelector;
  Selector readingWritingSelector;
  Broker broker;
  AtomicInteger totalAcceptd = new AtomicInteger(0);
  final int numberOfProcessingThread = 1;
  BlockingQueue<ProcessingThread> processingThreads = new LinkedBlockingQueue<>();

  public BrokerServer(int port, Broker broker){
    this.broker = broker;
    this.broker.setAllConnectionContext(allConnectionContext);
    this.port = port;
    try {
      acceptingSelector = Selector.open();
      readingWritingSelector = Selector.open();
      createThreads();
      startListening(port);
    }
    catch (IOException e) {
      e.printStackTrace();
    }
  }


  public static String connectionId(Socket socket){
    return socket.toString();
  }

  private void createThreads() throws IOException {
    for (int threadIndex = 0; threadIndex < numberOfProcessingThread; threadIndex++) {
      ProcessingThread processingThread = new ProcessingThread(broker, allConnectionContext);
      Thread thread = new Thread(processingThread, "Processing thread "+threadIndex);
      processingThreads.add(processingThread);
      thread.start();
    }
  }

  private void startListening(int port) throws IOException {
    ServerSocketChannel channel = ServerSocketChannel.open();
    channel.configureBlocking(false);
    channel.bind(getSocketAddress(), 1000);
    channel.register(acceptingSelector, SelectionKey.OP_ACCEPT);
    readingWritingThread();
    System.out.println("Server started!");
    selectAndDo(acceptingSelector);
  }

  private void readingWritingThread(){
    new Thread(new Runnable() {
      @Override
      public void run() {
        try {
          selectAndDo(readingWritingSelector);
        }
        catch (IOException e) {
          try {
            readingWritingSelector.close();
          }
          catch (IOException e1) {
            e1.printStackTrace();
          }
          e.printStackTrace();
        }
      }
    }, "Reading Writing Thread").start();
  }

  private void selectAndDo(Selector selector) throws IOException {
    while(true){
      int selectResult = selector.select(10);
      Iterator<SelectionKey> iterator = selector.selectedKeys().iterator();
      if(selectResult > 0) {
        while (iterator.hasNext()) {
          SelectionKey next = iterator.next();
          iterator.remove();
          SelectableChannel selectableChannel = next.channel();
          if (!next.isValid()) {
            continue;
          }
          if (next.isAcceptable()) {
            if (selectableChannel instanceof ServerSocketChannel) {
              ServerSocketChannel ssc = (ServerSocketChannel) selectableChannel;
              SocketChannel sc = ssc.accept();
              ProcessingThread processingThread =processingThreads.remove();
              processingThread.acceptNewConnection(sc);
              sc.configureBlocking(false);
              processingThreads.add(processingThread);
            }
          }
        }
      }
    }
  }


  private void configureServerSocket(ServerSocket  socket){

  }

  private SocketAddress getSocketAddress(){
    return new InetSocketAddress(this.port);
  }

  private TransportConnection onEachClientConnect(){
    return null;
  }
}
