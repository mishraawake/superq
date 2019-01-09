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

public class InfiniteConsumers {
  static long stime = System.currentTimeMillis();

  public static void main(String[] args) throws JMSException {
//"10.41.56.186"
    SMQConnectionFactory smqConnectionFactory = new SMQConnectionFactory("localhost", 1234);
    Connection connection = smqConnectionFactory.createConnection();
    Session session = connection.createSession(true, 1);

    AtomicInteger totalReceive = new AtomicInteger(0);
    session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
    Queue queue  = session.createQueue("myq");
    for (int consumers = 0; consumers < 1; consumers++) {
      MessageConsumer consumer = session.createConsumer(queue);
      final int consumerNo = consumers;
      consumer.setMessageListener(new MessageListener() {
        @Override
        public void onMessage(Message message) {
          receive(totalReceive, consumerNo);
        }
      });
    }
  }

  private static void sleep(int milisec){
    try {
      Thread.sleep(milisec);
    }
    catch (InterruptedException e) {
      e.printStackTrace();
    }
  }

  private static void receive(AtomicInteger totalReceive, int consumerNo){
    int totalREceive = totalReceive.incrementAndGet();
    //sleep(1);
    //System.out.println(consumerNo);
    if(totalREceive % 100000 == 0){
      System.out.println(totalREceive + "receives "+consumerNo +" in "+(System.currentTimeMillis() - stime) );
      stime = System.currentTimeMillis();

    }
  }
}
