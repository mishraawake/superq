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
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.superq.Broker;
import org.apache.superq.CallbackExecutor;

public class BrokerServer {

  int port;
  String hostName = "127.0.0.1";


  public Map<String, ConnectionContext> allConnectionContext = new ConcurrentHashMap<>();

  Selector selector;

  Broker broker;

  public BrokerServer(int port, Broker broker){
    this.broker = broker;
    this.port = port;
    try {
      selector = Selector.open();
      startListening(port);
    }
    catch (IOException e) {
      e.printStackTrace();
    }
  }


  public static String connectionId(Socket socket){
    return socket .toString();
  }

  private void startListening(int port) throws IOException {
    ServerSocketChannel channel = ServerSocketChannel.open();
    channel.configureBlocking(false);
    channel.bind(getSocketAddress());
    channel.register(selector, SelectionKey.OP_ACCEPT);
    System.out.println("Server started!");
    while(true){
      int selectResult = selector.select();
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
              // System.out.println(sc.getRemoteAddress());
              sc.configureBlocking(false);
              sc.socket().setTcpNoDelay(true);
              sc.socket().setKeepAlive(true);
              sc.socket().setReceiveBufferSize(1024 * 1024);
              SelectionKey key = sc.register(selector, SelectionKey.OP_READ);
              ConnectionContext connectionContext = new ConnectionContext(sc, key, broker);
              allConnectionContext.put(connectionId(sc.socket()), connectionContext);
            }
          }
          if (next.isReadable()) {
            SocketChannel sc = (SocketChannel) selectableChannel;
            ConnectionContext connectionContext = allConnectionContext.get(connectionId(sc.socket()));
            connectionContext.readRequest();
          }
          if (next.isWritable()) {
            SocketChannel sc = (SocketChannel) selectableChannel;
            ConnectionContext connectionContext = allConnectionContext.get(connectionId(sc.socket()));
            connectionContext.readRequest();
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
