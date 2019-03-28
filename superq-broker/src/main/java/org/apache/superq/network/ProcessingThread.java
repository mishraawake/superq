package org.apache.superq.network;

import java.io.IOException;
import java.nio.channels.SelectableChannel;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.superq.Broker;

public class ProcessingThread implements Runnable {

  Selector selector;
  AtomicInteger totalConnection = new AtomicInteger(0);
  private Broker broker;
  Map<String, ConnectionContext> allConnectionContext;

  public ProcessingThread(Broker broker, Map<String, ConnectionContext> allConnectionContext) throws IOException {
    selector = Selector.open();
    this.broker = broker;
    this.allConnectionContext = allConnectionContext;
  }

  BlockingQueue<SocketChannel> blockingQ = new LinkedBlockingQueue<>();

  public void acceptNewConnection(SocketChannel sc) throws IOException {
    totalConnection.incrementAndGet();
    configureSocket(sc);
    synchronized (blockingQ) {
      blockingQ.add(sc);
    }
    selector.wakeup();
  }

  private void configureSocket(SocketChannel sc) throws IOException {

    sc.configureBlocking(false);
    sc.socket().setTcpNoDelay(false);
    sc.socket().setKeepAlive(true);
    sc.socket().setReceiveBufferSize(1024 * 1024*1024);

  }

  public int getTotalConnection(){
    return totalConnection.get();
  }

  public void decrementConnection(){
    totalConnection.decrementAndGet();
  }

  @Override
  public void run() {

    while (true){
      try {
        int selectResult = selector.select(10);
        // register the already accepted.
        registerSocketInQ();
        Iterator<SelectionKey> iterator = selector.selectedKeys().iterator();
        if (selectResult > 0) {
          while (iterator.hasNext()) {
            SelectionKey next = iterator.next();
            iterator.remove();
            SelectableChannel selectableChannel = next.channel();
            if (!next.isValid()) {
              continue;
            }
            if (next.isAcceptable()) {
              if (selectableChannel instanceof ServerSocketChannel) {
                throw new IOException("No accepting socket expected here");
              }
            }
            if (next.isValid() && next.isReadable()) {
             // System.out.println("in reading"+System.currentTimeMillis());
              SocketChannel sc = (SocketChannel) selectableChannel;
              ConnectionContext connectionContext = null;
              connectionContext = (ConnectionContext) next.attachment();

              long stime = System.currentTimeMillis();
              connectionContext.readRequest();

              // Thread.sleep(1111);
              if ((System.currentTimeMillis() - stime) > 1)
                System.out.println("In reading " + (System.currentTimeMillis() - stime));
            }
            if (next.isValid() && next.isWritable()) {
              SocketChannel sc = (SocketChannel) selectableChannel;
              ConnectionContext connectionContext = null;
              connectionContext = (ConnectionContext) next.attachment();

              long stime = System.currentTimeMillis();
              connectionContext.writeResponse();
              if ((System.currentTimeMillis() - stime) > 1)
                System.out.println("In writing " + (System.currentTimeMillis() - stime));

            }
          }
        }
      } catch (IOException e){
        throw new RuntimeException(e);
      }
    }

  }

  private void registerSocketInQ() throws IOException {
    if(!blockingQ.isEmpty())
    synchronized (blockingQ) {
      for (SocketChannel socketChannel : blockingQ){
          SelectionKey key = socketChannel.register(selector, SelectionKey.OP_READ);
          String connectionId = BrokerServer.connectionId(socketChannel.socket());
          ConnectionContext connectionContext = new ConnectionContext(socketChannel, key, broker, connectionId);
          key.attach(connectionContext);
          allConnectionContext.put(connectionId, connectionContext);
      }
      blockingQ.clear();
    }
  }
}
