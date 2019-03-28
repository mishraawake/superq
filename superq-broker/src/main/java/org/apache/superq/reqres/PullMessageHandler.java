package org.apache.superq.reqres;

import java.io.IOException;

import org.apache.superq.PullMessage;
import org.apache.superq.network.ConnectionContext;
import org.apache.superq.network.SessionContext;
import org.apache.superq.outgoing.SBConsumer;
import org.apache.superq.outgoing.SBConsumerDefault;

public class PullMessageHandler implements RequestHandler<PullMessage> {
  @Override
  public void handle(PullMessage pullMessage, ConnectionContext connectionContext) {
    if(connectionContext.getInfo().getConnectionId() != pullMessage.getConnectionId()){
      // error condition
      throw new IllegalStateException("Producer connectionId "+pullMessage.getConnectionId()+
                                              " does not match with actual connectionID " +
                                              + connectionContext.getInfo().getConnectionId());
    }

    SessionContext sqSession = connectionContext.getSession(pullMessage.getSessionId());

    if(sqSession == null){
      throw new IllegalStateException("No session for the producer connectionId "+pullMessage.getId());
    }

    SBConsumer sbConsumer = new SBConsumerDefault(pullMessage, sqSession);
    if(sqSession.getConsumers().putIfAbsent(pullMessage.getId(), pullMessage) != null){
      // handle duplicate
    }
    try {
      connectionContext.getBroker().pullMessage(pullMessage);
    }
    catch (IOException e) {
      e.printStackTrace();
    }
  }
}
