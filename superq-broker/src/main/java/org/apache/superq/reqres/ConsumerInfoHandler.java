package org.apache.superq.reqres;

import java.io.IOException;

import org.apache.superq.ConsumerInfo;
import org.apache.superq.network.ConnectionContext;
import org.apache.superq.network.SessionContext;
import org.apache.superq.outgoing.SBConsumer;
import org.apache.superq.outgoing.SBConsumerDefault;

public class ConsumerInfoHandler implements RequestHandler<ConsumerInfo> {
  @Override
  public void handle(ConsumerInfo consumerInfo, ConnectionContext connectionContext) {

    if(connectionContext.getInfo().getConnectionId() != consumerInfo.getConnectionId()){
      // error condition
      throw new IllegalStateException("Producer connectionId "+consumerInfo.getConnectionId()+
                                              " does not match with actual connectionID " +
                                              + connectionContext.getInfo().getConnectionId());
    }

    SessionContext sqSession = connectionContext.getSession(consumerInfo.getSessionId());

    if(sqSession == null){
      throw new IllegalStateException("No session for the producer connectionId "+consumerInfo.getId());
    }

    SBConsumer sbConsumer = new SBConsumerDefault(consumerInfo, sqSession);
    if(sqSession.getConsumers().putIfAbsent(consumerInfo.getId(), consumerInfo) != null){
      // handle duplicate
    }
    try {
      connectionContext.getBroker().addConsumer(sbConsumer);
    }
    catch (IOException e) {
      e.printStackTrace();
    }
  }
}
