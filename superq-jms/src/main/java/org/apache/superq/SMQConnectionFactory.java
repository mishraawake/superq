package org.apache.superq;

import java.io.IOException;
import java.net.BindException;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.nio.channels.CancelledKeyException;
import java.nio.channels.SelectableChannel;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.SocketChannel;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicLong;
import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.JMSContext;
import javax.jms.JMSException;

public class SMQConnectionFactory implements ConnectionFactory {


  String host;
  int port;
  AtomicLong connectionId = new AtomicLong(0);
  Selector selector;
  Selector connectionSelector;

  InetSocketAddress inetSocketAddress;
  Connection testConnection;
  BlockingQueue<SocketChannel> socketChannelQuedForConnection = new LinkedBlockingQueue<>();
  Map<SocketChannel, SMQConnection> channelToConnection = new ConcurrentHashMap<>();

  public SMQConnectionFactory(String host, int port) throws IOException {
    Class c = WiredObjectFactory.classTypes[0];
    long stime = System.currentTimeMillis();
    this.host = host;
    this.port = port;
    SocketChannel.open();
    this.selector = Selector.open();
    this.connectionSelector = Selector.open();
    inetSocketAddress = new InetSocketAddress(this.host, this.port);
    NetworkThread networkThread = new NetworkThread(this.selector);
    Thread thread = new Thread(networkThread, "Network Thread");
    thread.start();
    networkThread = new NetworkThread(this.connectionSelector);
    thread = new Thread(networkThread, "Network Selector Thread");
    thread.start();
  }

  @Override
  public  Connection createConnection() throws JMSException {
    try {
     // Selector selector = Selector.open();
      long stime = System.currentTimeMillis();
      SocketChannel socketChannel = connect();
      //System.out.println("Connect time1 "+(System.currentTimeMillis() - stime));
      long nextconnectionId = connectionId.incrementAndGet();

      SMQConnection connection =  new SMQConnection(socketChannel, nextconnectionId, this.selector, this.connectionSelector);
      channelToConnection.put(socketChannel, connection);
     // System.out.println("Connect time2 "+(System.currentTimeMillis() - stime));
      //System.out.println("socket channel "+socketChannel.hashCode());
      synchronized (socketChannelQuedForConnection) {
        socketChannelQuedForConnection.add(socketChannel);
      }

      connection.registerForConnect();
      //System.out.println("Connect time3 "+(System.currentTimeMillis() - stime));
      connection.start();
      if(System.currentTimeMillis() - stime > 15000)
      System.out.println("start time3 "+(System.currentTimeMillis() - stime));
      return connection;
    } catch (IOException ioe){
      JMSException jmse =  new JMSException("Broker host could not be found");
      jmse.setLinkedException(ioe);
      throw jmse;
    }
  }

  private synchronized SocketChannel getSocketChannel() throws IOException {
    SocketChannel socketChannel = SocketChannel.open();
    socketChannel.configureBlocking(false);
    socketChannel.connect(inetSocketAddress);
    return socketChannel;
  }

  private SocketChannel connect() throws IOException {
    SocketChannel socketChannel = getSocketChannel();
    long stime = System.currentTimeMillis();
    socketChannel.socket().setSendBufferSize(1024*1024*10);
    socketChannel. socket().setTcpNoDelay(true);
    socketChannel.socket().setKeepAlive(true);
    if(System.currentTimeMillis() - stime > 1000)
    System.out.println("connection opening "+(System.currentTimeMillis() - stime));
    return socketChannel;
  }

  @Override
  public Connection createConnection(String userName, String password) throws JMSException {
    return null;
  }

  @Override
  public JMSContext createContext() {
    return null;
  }

  @Override
  public JMSContext createContext(String userName, String password) {
    return null;
  }

  @Override
  public JMSContext createContext(String userName, String password, int sessionMode) {
    return null;
  }

  @Override
  public JMSContext createContext(int sessionMode) {
    return null;
  }

  public  Connection bareConnection() throws JMSException {
    SMQConnection connection = null;
    return null;
  }

  public static void main(String[] args) throws IOException {

    long stime = System.currentTimeMillis();
    SocketChannel socketChannel = SocketChannel.open();
    Selector.open();
    SMQConnectionFactory smqConnectionFactory = new SMQConnectionFactory("127.0.0.1", 1234);
    //System.out.println(System.currentTimeMillis() - stime);

  }

  public void close() throws JMSException {
    selector.wakeup();
    connectionSelector.wakeup();
    try {
      selector.close();
      connectionSelector.close();
    }

    catch (IOException e) {
      e.printStackTrace();
    }
  }



  private void infiniteSelect(Selector selector) throws IOException, JMSException {
    long gstime = 0;
    int totalSel = 0;
    int size = 0;
    while(true){
      try {
        if(selector == this.selector){
          int selectResult = selector.select(200);
        } else {
          int selectResult = selector.select(20);
        }
        registerChannels();


      }catch (Exception e){
        return;
      }
      if(selector.isOpen()) {
        gstime = System.currentTimeMillis();
        size = selector.selectedKeys().size();
        Iterator<SelectionKey> iterator = selector.selectedKeys().iterator();

        long whileTime = System.currentTimeMillis();
        while (iterator.hasNext()) {
          totalSel++;
          SelectionKey next = iterator.next();
          iterator.remove();
          synchronized (next){
            SelectableChannel selectableChannel = next.channel();
            if (!next.isValid()) {
              //System.out.println("Invalid.."+Thread.currentThread().getName());
              continue;
            }
            if (next.isValid() && next.isReadable()) {
              next.interestOps(next.interestOps() & ~ SelectionKey.OP_READ);
              System.out.println("Reading.."+System.currentTimeMillis());
              long stime = System.currentTimeMillis();
              SocketChannel sc = (SocketChannel) selectableChannel;
              SMQConnection connection  = (SMQConnection)next.attachment();
              connection.wakeForRead();
              //  connection.readRequest(sc);
              if(System.currentTimeMillis() - stime > 10)
                System.out.println("REading ======================="+(System.currentTimeMillis() - stime) );
            }
            try {
              if (next.isValid() && next.isWritable()) {
                next.interestOps(next.interestOps() & ~ SelectionKey.OP_WRITE);
                SMQConnection connection  = (SMQConnection)next.attachment();
                //System.out.println("Writing.." + next);
                long stime = System.currentTimeMillis();
                SocketChannel sc = (SocketChannel) selectableChannel;
                connection.wakeForWrite();
                // connection.writeJumbo(sc);
                if(System.currentTimeMillis() - stime > 10)
                  System.out.println("Writing ===================" + (System.currentTimeMillis() - stime) );

              }

              if(next.isValid() && next.isConnectable()){
                SocketChannel sc = (SocketChannel) selectableChannel;
                SMQConnection connection  = channelToConnection.get(sc);
                next.interestOps(0);
                System.out.println("connected.."+Thread.currentThread().getName() + " "+System.currentTimeMillis() );
                long stime = System.currentTimeMillis();
                if(connection != null) {
                  connection.notifyToConnect(sc);
                } else {
                  //System.out.println("no socket channel "+sc.hashCode());
                  throw new RuntimeException("No Channle found");
                }
              }
            }catch (CancelledKeyException e){
            }
          }
        }
        //System.out.println("Time in while "+(System.currentTimeMillis() - whileTime));
      } else {
        return;
      }
      if((System.currentTimeMillis() - gstime) > 1000)
        System.out.println("Total time "+(System.currentTimeMillis() - gstime) +" "+size + " "+Thread.currentThread().getName() + "--- "+(totalSel));
    }
  }

  private void registerChannels() throws IOException {
    synchronized (socketChannelQuedForConnection) {
      for(SocketChannel socketChannel : socketChannelQuedForConnection){
        socketChannel.register(connectionSelector, SelectionKey.OP_CONNECT);
      }
      socketChannelQuedForConnection.clear();
    }
  }

  public class NetworkThread implements Runnable {


    Selector selector;

    NetworkThread(Selector selector){
      this.selector = selector;
    }
    @Override
    public void run() {

      //  //System.out.println("NetworkThread thread started "+SMQConnection.this.getConnectionId());
      try {
        infiniteSelect(this.selector);
      }
      catch (IOException | JMSException e) {
        e.printStackTrace();
      }
    }
  }
}
