package org.apache.superq.reqres;

import java.io.IOException;

import org.apache.superq.ProducerInfo;
import org.apache.superq.incoming.SBProducerContext;
import org.apache.superq.network.ConnectionContext;
import org.apache.superq.network.SessionContext;

public class ProducerInfoHandler implements RequestHandler<ProducerInfo> {

  @Override
  public void handle(ProducerInfo info, ConnectionContext connectionContext) {
    if(connectionContext.getInfo().getConnectionId() != info.getConnectionId()){
      // error condition
      throw new IllegalStateException("Producer connectionId "+info.getConnectionId()+
                                              " does not match with actual connectionID " +
                                              + connectionContext.getInfo().getConnectionId());
    }

    SessionContext sqSession = connectionContext.getSession(info.getSessionId());

    if(sqSession == null){
      throw new IllegalStateException("No session for the producer connectionId "+info.getId());
    }

    SBProducerContext sbProducerContext = new SBProducerContext(info);
    sbProducerContext.setSessionContext(sqSession);
    sbProducerContext.setConnectionContext(connectionContext);
    if(sqSession.getProducers().putIfAbsent(info.getId(), sbProducerContext) != null){
      // handle duplicate
    }

   // connectionContext.setPr(new MaxReaderPartialRequest());
    try {
      connectionContext.sendAsyncPacket(info);
    }
    catch (IOException e) {
      e.printStackTrace();
    }
  }
}
