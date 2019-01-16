package org.apache.superq;

import java.io.IOException;
import java.net.BindException;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.nio.channels.SocketChannel;
import java.util.concurrent.atomic.AtomicLong;
import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.JMSContext;
import javax.jms.JMSException;

public class SMQConnectionFactory implements ConnectionFactory {


  String host;
  int port;
  AtomicLong connectionId = new AtomicLong(0);

  public SMQConnectionFactory(String host, int port){
    this.host = host;
    this.port = port;
  }

  @Override
  public Connection createConnection() throws JMSException {
    try {
      SMQConnection connection = new SMQConnection(connect(), connectionId.incrementAndGet());
      return connection;
    } catch (IOException | InterruptedException ioe){
      JMSException jmse =  new JMSException("Broker host could not be found");
      jmse.setLinkedException(ioe);
      throw jmse;
    }
  }

  private SocketChannel connect() throws IOException, InterruptedException {
    int count = 0;
    BindException betoThrow = null;
    while(count++  < 1000) {
      try {
        SocketChannel socketChannel = SocketChannel.open();
        socketChannel.configureBlocking(false);
        boolean connected = socketChannel.connect(new InetSocketAddress(this.host, this.port));
        while(!socketChannel.finishConnect()){
          Thread.yield();
        }
        socketChannel.socket().setSendBufferSize(1024*1024*10);
        socketChannel. socket().setTcpNoDelay(false);
        return socketChannel;
      }
      catch (BindException be) {
        betoThrow = be;
        Thread.sleep(10);
      }
    }
    throw betoThrow;
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
}
