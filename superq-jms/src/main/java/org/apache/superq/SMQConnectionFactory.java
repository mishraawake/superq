package org.apache.superq;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Socket;
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
      Socket socket = new Socket();
      socket.connect(new InetSocketAddress(this.host, this.port));
      SMQConnection connection = new SMQConnection(socket, connectionId.incrementAndGet());
      return connection;
    } catch (IOException ioe){
      JMSException jmse =  new JMSException("Broker host could not be found");
      jmse.setLinkedException(ioe);
      throw jmse;
    }
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
