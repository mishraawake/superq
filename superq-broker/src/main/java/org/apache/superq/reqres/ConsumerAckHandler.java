package org.apache.superq.reqres;

import java.io.IOException;

import javax.jms.JMSException;

import org.apache.superq.ConsumerAck;
import org.apache.superq.SMQMessage;
import org.apache.superq.network.ConnectionContext;
import org.apache.superq.network.SessionContext;
import org.apache.superq.storage.SBQueue;

public class ConsumerAckHandler implements RequestHandler<ConsumerAck> {
  @Override
  public void handle(ConsumerAck consumerAck, ConnectionContext connectionContext) {
    if(connectionContext.getInfo().getConnectionId() != consumerAck.getConnectionId()){
      // error condition
      throw new IllegalStateException("Producer connectionId "+consumerAck.getConnectionId()+
                                              " does not match with actual connectionID " +
                                              + connectionContext.getInfo().getConnectionId());
    }

    SessionContext sqSession = connectionContext.getSession(consumerAck.getSessionId());

    if(sqSession == null){
      throw new IllegalStateException("No session for the producer connectionId "+consumerAck.getId());
    }


    try {
      SBQueue<SMQMessage> queue = connectionContext.getBroker().getQueue(consumerAck.getQid());
      queue.ackMessage(consumerAck);
    }
    catch (IOException e) {
      e.printStackTrace();
    }
    catch (JMSException e) {
      e.printStackTrace();
    }
  }
}
