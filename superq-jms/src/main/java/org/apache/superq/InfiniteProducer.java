package org.apache.superq;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicInteger;
import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.DeliveryMode;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageListener;
import javax.jms.MessageProducer;
import javax.jms.Queue;
import javax.jms.Session;
import javax.jms.TextMessage;

public class InfiniteProducer {
  public static void main(String[] args) throws JMSException, IOException {
    //10.41.56.186
    SMQConnectionFactory smqConnectionFactory = new SMQConnectionFactory("localhost", 1234);
    InfiniteProducer infiniteProducer = new InfiniteProducer();
    smqConnectionFactory.createConnection();
    infiniteProducer.produce(1,smqConnectionFactory.createConnection());
  }

  protected void produce(int numberOfMessage, Connection connection) throws JMSException {
    long stime = System.currentTimeMillis();
   // Connection connection = connectionFactory.createConnection();
    //System.out.println("starting.."+(System.currentTimeMillis() - stime));
    stime  = System.currentTimeMillis();
   // connection.start();
    Session session = connection.createSession(true, 1);
    System.out.println("total time === "+(System.currentTimeMillis() - stime)+" total = "+" "+((SMQConnection)connection).getConnectionId());
    Queue queue = session.createQueue("myq");
    System.out.println("total time === "+(System.currentTimeMillis() - stime)+" total = "+" "+((SMQConnection)connection).getConnectionId());
    MessageProducer producer = session.createProducer(queue);
    System.out.println("total time === "+(System.currentTimeMillis() - stime)+" total = "+" "+((SMQConnection)connection).getConnectionId());

    int count = 0;
    //stime  = System.currentTimeMillis();
    for (int messageCount = 0; messageCount < numberOfMessage; messageCount++) {
      TextMessage message = session.createTextMessage();
      String str = getText(10);
      message.setText( str + messageCount);
      message.setJMSDeliveryMode(DeliveryMode.PERSISTENT);
      producer.send(message, DeliveryMode.PERSISTENT, 4, 1000);
      ++count;
    }
    System.out.println("total time === "+(System.currentTimeMillis() - stime)+" total = "+" "+((SMQConnection)connection).getConnectionId());

    session.commit();
    System.out.println("total time === ----"+(System.currentTimeMillis() - stime)+" total = "+count+" "+((SMQConnection)connection).getConnectionId());
  //  connection.close();
    if(System.currentTimeMillis() - stime > 1500)
      System.out.println("total time === "+(System.currentTimeMillis() - stime)+" total = "+count+" "+((SMQConnection)connection).getConnectionId());
  }

  protected String getText(int msg){
    StringBuilder str = new StringBuilder();
    for (int count = 0; count < msg; count++) {
      str.append("First message ");
    }
    return str.toString();
  }

  protected void sleep(int milis){
    try {
      Thread.sleep(milis);
    }
    catch (InterruptedException e) {
      e.printStackTrace();
    }
  }
}


