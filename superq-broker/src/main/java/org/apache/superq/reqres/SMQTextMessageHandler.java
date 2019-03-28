package org.apache.superq.reqres;

import java.io.IOException;
import javax.jms.JMSException;

import org.apache.superq.QueueInfo;
import org.apache.superq.SMQDestination;
import org.apache.superq.SMQMessage;
import org.apache.superq.SMQTextMessage;
import org.apache.superq.incoming.SBProducerContext;
import org.apache.superq.network.ConnectionContext;

public class SMQTextMessageHandler implements RequestHandler<SMQTextMessage> {


  @Override
  public void handle(SMQTextMessage message, ConnectionContext connectionContext) {

    //System.out.println("getting message");
    try {
      SBProducerContext producerContext =  handleError(message, connectionContext);
      if(producerContext != null){
        connectionContext.getBroker().appendMessage(message, producerContext);
      }
    }
    catch (JMSException | IOException e) {
      e.printStackTrace();
    }
  }

  private SBProducerContext handleError(SMQMessage message, ConnectionContext connectionContext) throws JMSException, IOException {
    SBProducerContext producerContext = connectionContext.getSession(message.getSessionId()).getProducers().get(message.getProducerId());
    int destinationId = ((SMQDestination)message.getJMSDestination()).getDestinationId();
    QueueInfo queueInfo = connectionContext.getBroker().getQueueInfo(destinationId);
    message.setJMSDestination(queueInfo);
    if(message.getJMSReplyTo() != null){
       destinationId = ((SMQDestination)message.getJMSReplyTo()).getDestinationId();
       queueInfo = connectionContext.getBroker().getQueueInfo(destinationId);
       message.setJMSReplyTo(queueInfo);
    }
    return producerContext;
  }
}
