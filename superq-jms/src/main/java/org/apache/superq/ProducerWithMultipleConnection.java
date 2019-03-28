package org.apache.superq;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicInteger;
import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.JMSException;

public class ProducerWithMultipleConnection extends InfiniteProducer {

  private AtomicInteger totalTh = new AtomicInteger(0);
  static SMQConnectionFactory smqConnectionFactory;
  final private int totalThread  = 10
          ;
  public static void main(String[] args) throws IOException, JMSException {
    //10.41.56.186
    smqConnectionFactory = new SMQConnectionFactory("127.0.0.1", 1234);
   // smqConnectionFactory.createConnection();
    ProducerWithMultipleConnection infiniteProducer = new ProducerWithMultipleConnection();
    infiniteProducer.startThreads(smqConnectionFactory);
   // smqConnectionFactory.close();
  }


  public void startThreads(ConnectionFactory connectionFactory){
    for (int threadIndex = 0; threadIndex < totalThread; threadIndex++) {
      produceInSeparateThread(1,threadIndex, connectionFactory);
    }
  }

  public void produceInSeparateThread(int timesProduced, int producerNumber, ConnectionFactory connectionFactory){
    new Thread(new Runnable() {
      @Override
      public void run() {
        try {
          int count = 0;
          long stime = System.currentTimeMillis();
          Connection connection = null;
          int error = 0;
          while (connection == null) {
            try {
              connection = connectionFactory.createConnection();
            }
            catch (JMSException e) {
              ++error;
            }
          }
            if ((System.currentTimeMillis() - stime) > 1)
              System.out.println("Connect time " + producerNumber + " in time " + (System.currentTimeMillis() - stime) + " " + ((SMQConnection) connection).key);
            while (count++ < timesProduced) {
              //connectionFactory
              produce(1, connection);
              // sleep(10);
            }
            if ((System.currentTimeMillis() - stime) > 1000)
              System.out.println("Finished " + producerNumber + " in time " + (System.currentTimeMillis() - stime) + " " + ((SMQConnection) connection).key);
          System.out.println("Closing ........... "+Thread.currentThread().getName());
            connection.close();

          totalTh.incrementAndGet();
          if(totalTh.get() == totalThread){
            System.out.println("Finished Total...............Good"+totalTh + " error "+error);
            smqConnectionFactory.close();
          } else {
            System.out.println("Finished Total...............Good"+totalTh+ " error "+error);
          }
        }
        catch (JMSException e) {
          System.out.println(" "+Thread.currentThread().getName());
          e.printStackTrace();
          e.getLinkedException().printStackTrace();
          System.out.println("Exiting..");
          System.exit(1);
          return;
        }
      }
    }, "Producer Thread "+producerNumber).start();
  }
}
