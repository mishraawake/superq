package org.apache.superq.reqres;

import java.io.IOException;
import javax.jms.JMSException;

import org.apache.superq.SMQMessage;
import org.apache.superq.SMQTextMessage;
import org.apache.superq.incoming.SBProducerContext;
import org.apache.superq.network.ConnectionContext;

public class SMQTextMessageHandler implements RequestHandler<SMQTextMessage> {
  @Override
  public void handle(SMQTextMessage message, ConnectionContext connectionContext) {
    SBProducerContext producerContext =  handleError(message, connectionContext);
    if(producerContext != null){
      try {
        connectionContext.getBroker().appendMessage(message, producerContext);
      }
      catch (JMSException | IOException e) {
        e.printStackTrace();
      }
    }
  }

  private SBProducerContext handleError(SMQMessage message, ConnectionContext connectionContext){
    return null;
  }
}
