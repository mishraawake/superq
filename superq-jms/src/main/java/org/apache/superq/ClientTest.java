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

public class ClientTest {
  public static void main(String[] args) throws JMSException {
    SMQConnectionFactory smqConnectionFactory = new SMQConnectionFactory("localhost", 1234);
    Connection connection = smqConnectionFactory.createConnection();
    Session session = connection.createSession(true, 1);
    Queue queue = session.createQueue("myq");
    MessageProducer producer = session.createProducer(queue);
    for (int messageCount = 0; messageCount < 100; messageCount++) {
      TextMessage message = session.createTextMessage();
      message.setText("First message"+messageCount);
      message.setJMSDeliveryMode(DeliveryMode.PERSISTENT);
      producer.send(message, DeliveryMode.PERSISTENT, 4, 1000);
    }
    session.commit();

    AtomicInteger totalReceive = new AtomicInteger(0);
    session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
    queue = session.createQueue("myq");
    MessageConsumer consumer = session.createConsumer(queue);
    consumer.setMessageListener(new MessageListener() {
      @Override
      public void onMessage(Message message) {
        try {
          System.out.println(((TextMessage)message).getText() + " -- consumer 1-- "+totalReceive.incrementAndGet());
        }
        catch (JMSException e) {
          e.printStackTrace();
        }
      }
    });


    consumer = session.createConsumer(queue);
    consumer.setMessageListener(new MessageListener() {
      @Override
      public void onMessage(Message message) {
        try {
          System.out.println(((TextMessage)message).getText()+ " -- consumer 2 -- "+totalReceive.incrementAndGet());

        }
        catch (JMSException e) {
          e.printStackTrace();
        }
      }
    });
  }
}
