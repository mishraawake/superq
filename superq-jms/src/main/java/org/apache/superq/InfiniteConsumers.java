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
    for (int counsumerCount = 0; counsumerCount < 1000; counsumerCount++) {

    session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
      final int cc = counsumerCount;
      Queue queue  = session.createQueue("myq");
      MessageConsumer consumer = session.createConsumer(queue);
      consumer.setMessageListener(new MessageListener() {
        @Override
        public void onMessage(Message message) {
          receive(totalReceive, cc);
        }
      });
    }


    /*
  int count =0 ;
  while (true){
    consumer.receive();
    ++count;
    System.out.println(count);
  }
  */

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
    sleep(10);
    if(totalREceive == 1){
      stime = System.currentTimeMillis();
      System.out.println(totalREceive + "receives");
    }
    //System.out.println(consumerNo);
    if(totalREceive % 10000 == 0){
      System.out.println(totalREceive + "receives "+consumerNo +" in "+(System.currentTimeMillis() - stime) + " "+Thread.currentThread().getName() );
    }
  }
}

