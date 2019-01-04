package org.apache.superq.db;

import java.io.IOException;
import javax.jms.JMSException;

import org.apache.superq.Broker;
import org.apache.superq.ConsumerInfo;
import org.apache.superq.QueueInfo;
import org.apache.superq.outgoing.SBConsumer;
import org.apache.superq.outgoing.SBConsumerDefault;
import org.apache.superq.storage.SBQueueDefault;
import org.junit.Test;

public class QueueTest extends AbstractTest {

  @Test
  public void testMessageRelay() throws IOException, JMSException {
    Broker broker = new Broker();
    QueueInfo queueInfo = new QueueInfo();
    queueInfo.setQueueName(qname);
    queueInfo.setId(1);
    SBQueueDefault sbQueueDefault = new SBQueueDefault(broker, queueInfo, fileDatabase);
  }

  private SBConsumer addConsumers(){
    ConsumerInfo consumerInfo = new ConsumerInfo();
    consumerInfo.setQid(1);
    consumerInfo.setConnectionId(1);
    SBConsumer consumer = new SBConsumerDefault(consumerInfo, null);
    return consumer;
  }
}
