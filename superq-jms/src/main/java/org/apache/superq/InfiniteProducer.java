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
    SMQConnectionFactory smqConnectionFactory = new SMQConnectionFactory("localhost", 1234);
    Connection connection = smqConnectionFactory.createConnection();
    Session session = connection.createSession(true, 1);
    Queue queue = session.createQueue("myq");
    MessageProducer producer = session.createProducer(queue);
    while(true) {
      long stime  = System.currentTimeMillis();
      for (int messageCount = 0; messageCount < 100000; messageCount++) {
        TextMessage message = session.createTextMessage();
        message.setText("First message" + messageCount);
        message.setJMSDeliveryMode(DeliveryMode.PERSISTENT);
        producer.send(message, DeliveryMode.PERSISTENT, 4, 1000);
      }
      session.commit();
      System.out.println(System.currentTimeMillis() - stime);
    }
  }
}
