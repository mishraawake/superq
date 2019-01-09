package org.apache.superq;

import java.util.concurrent.atomic.AtomicInteger;
import javax.jms.Connection;
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
  public static void main(String[] args) throws JMSException {
    //10.41.56.186
    SMQConnectionFactory smqConnectionFactory = new SMQConnectionFactory("localhost", 1234);
    Connection connection = smqConnectionFactory.createConnection();
    Session session = connection.createSession(true, 1);
    Queue queue = session.createQueue("myq");
    MessageProducer producer = session.createProducer(queue);
    int count = 0;
    while(true) {
      long stime  = System.currentTimeMillis();
      for (int i = 0; i < 1; i++) {
        for (int messageCount = 0; messageCount < 100000; messageCount++) {
          TextMessage message = session.createTextMessage();
          String str = getText(10);
          message.setText( str + messageCount);
          message.setJMSDeliveryMode(DeliveryMode.PERSISTENT);
          producer.send(message, DeliveryMode.PERSISTENT, 4, 1000);
          ++count;
        }
        session.commit();
      }
      sleep(500);
      System.out.println("total time"+(System.currentTimeMillis() - stime)+" total = "+count);
    }
  }
  private static String getText(int msg){
    StringBuilder str = new StringBuilder();
    for (int count = 0; count < msg; count++) {
      str.append("First message ");
    }
    return str.toString();
  }

  private static void sleep(int milis){
    try {
      Thread.sleep(milis);
    }
    catch (InterruptedException e) {
      e.printStackTrace();
    }
  }
}


