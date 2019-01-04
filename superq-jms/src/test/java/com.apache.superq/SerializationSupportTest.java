package com.apache.superq;

import java.io.IOException;
import java.util.Date;

import javax.jms.DeliveryMode;
import javax.jms.Destination;
import javax.jms.JMSException;

import org.apache.superq.ConnectionInfo;
import org.apache.superq.QueueInfo;
import org.apache.superq.SMQDestination;
import org.apache.superq.SMQTextMessage;
import org.apache.superq.SessionInfo;
import org.junit.Assert;
import org.junit.Test;

public class SerializationSupportTest {

  @Test
  public void serializeQInfo() throws IOException, JMSException {
    QueueInfo queueInfo = new QueueInfo();
    queueInfo.setQueueName("myqueu");
    queueInfo.setId(2);
    queueInfo.setPacketId(3);
    QueueInfo afterSerialization  = new QueueInfo();
    afterSerialization.acceptByteBuffer(queueInfo.getBuffer());
    System.out.println(afterSerialization);
    Assert.assertEquals(afterSerialization, queueInfo);
  }

  @Test
  public void serializeConnectionInfo() throws IOException, JMSException {
    ConnectionInfo connectionInfo = new ConnectionInfo();
    connectionInfo.setPacketId(2);
    connectionInfo.setConnectionId(0);

    ConnectionInfo afterSerialization  = new ConnectionInfo();
    afterSerialization.acceptByteBuffer(connectionInfo.getBuffer());
    connectionInfo = new ConnectionInfo();

    connectionInfo.acceptByteBuffer(afterSerialization.getBuffer());
    System.out.println(connectionInfo);
    Assert.assertEquals(afterSerialization, connectionInfo);
  }

  @Test
  public void serializeSessionInfo() throws IOException, JMSException {
    SessionInfo sessionInfo = new SessionInfo();
    sessionInfo.setSessionId(1);
    sessionInfo.setConnectionId(2);

    SessionInfo afterSerialization  = new SessionInfo();
    afterSerialization.acceptByteBuffer(sessionInfo.getBuffer());
    System.out.println(afterSerialization);
    Assert.assertEquals(afterSerialization, sessionInfo);
  }

  @Test
  public void serializeQInfoWithNull() throws IOException, JMSException {
    QueueInfo queueInfo = new QueueInfo();
    //queueInfo.setId(2);
    //queueInfo.setPacketId(-3);
    queueInfo.setQueueName("queuename");
    QueueInfo afterSerialization  = new QueueInfo();
    afterSerialization.acceptByteBuffer(queueInfo.getBuffer());
    System.out.println(afterSerialization);
    Assert.assertEquals(afterSerialization, queueInfo);
  }

  @Test
  public void serializeMessageText() throws IOException, JMSException {
    SMQTextMessage textMessage = new SMQTextMessage();
    textMessage.setText("My text pankaj");
    textMessage.setJmsMessageLongId(1l);
    textMessage.setProducerId(2l);
    textMessage.setSessionId(3l);
    textMessage.setConnectionId(4l);
    textMessage.setConsumerId(5l);
    textMessage.setGroupId("Group");
    textMessage.setGroupSegId(1);
    textMessage.setJMSCorrelationID("correlation");
    textMessage.setJMSDeliveryMode(DeliveryMode.PERSISTENT);
    textMessage.setJMSDeliveryTime(new Date().getTime());
    textMessage.setJMSDestination(new SMQDestination() {
      @Override
      public int getDestinationId() {
        return 10;
      }
    });
    textMessage.setJMSExpiration(new Date().getTime());
   // textMessage.setJMSMessageID("jmsmessageId");
    textMessage.setJMSPriority(9);
    textMessage.setJMSRedelivered(false);
    textMessage.setJMSReplyTo(new SMQDestination() {
      @Override
      public int getDestinationId() {
        return 11;
      }

    });
    textMessage.setJMSTimestamp(new Date().getTime());
    textMessage.setTransactionId(100);
    //queueInfo.setId(2);
    //queueInfo.setPacketId(-3);
    SMQTextMessage afterSerialization  = new SMQTextMessage();
    afterSerialization.acceptByteBuffer(textMessage.getBuffer());
    afterSerialization.setTransactionId(100);
    System.out.println(afterSerialization);
    Assert.assertEquals(afterSerialization, textMessage);
  }

}
