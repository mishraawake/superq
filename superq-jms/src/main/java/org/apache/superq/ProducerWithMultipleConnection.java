package org.apache.superq;

import javax.jms.ConnectionFactory;
import javax.jms.JMSException;

public class ProducerWithMultipleConnection extends InfiniteProducer {

  public static void main(String[] args)  {
    //10.41.56.186
    SMQConnectionFactory smqConnectionFactory = new SMQConnectionFactory("localhost", 1234);
    ProducerWithMultipleConnection infiniteProducer = new ProducerWithMultipleConnection();
    infiniteProducer.startThreads(smqConnectionFactory);
  }

  public void produceWithMulCon(ConnectionFactory connectionFactory) throws JMSException {
    for (int connection = 0; connection < 100; connection++) {
      produce(100, connectionFactory);
    }
  }

  public void startThreads(ConnectionFactory connectionFactory){
    for (int threadIndex = 0; threadIndex < 10; threadIndex++) {
      produceInSeparateThread(10000,threadIndex, connectionFactory);
    }
  }

  public void produceInSeparateThread(int timesProduced, int producerNumber, ConnectionFactory connectionFactory){
    System.out.println("Starting "+producerNumber);
    new Thread(new Runnable() {
      @Override
      public void run() {
        try {
          int count = 0;
          long stime = System.currentTimeMillis();
          while(count++ < timesProduced) {
            produce(1000, connectionFactory);
           // sleep(10);
          }
          System.out.println("Finished "+producerNumber + " in time "+(System.currentTimeMillis() - stime));
        }
        catch (JMSException e) {
          e.printStackTrace();
          e.getLinkedException().printStackTrace();
        }
      }
    }, "Producer Thread "+producerNumber).start();
  }
}
