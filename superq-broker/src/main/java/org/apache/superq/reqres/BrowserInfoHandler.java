package org.apache.superq.reqres;

import java.io.IOException;
import javax.jms.JMSException;

import org.apache.superq.BrowserInfo;
import org.apache.superq.incoming.SBProducerContext;
import org.apache.superq.network.ConnectionContext;
import org.apache.superq.network.SessionContext;

public class BrowserInfoHandler implements RequestHandler<BrowserInfo> {

  @Override
  public void handle(BrowserInfo browserInfo, ConnectionContext connectionContext) {
    if(connectionContext.getInfo().getConnectionId() != browserInfo.getConnectionId()){
      // error condition
      throw new IllegalStateException("Producer connectionId "+browserInfo.getConnectionId()+
                                              " does not match with actual connectionID " +
                                              + connectionContext.getInfo().getConnectionId());
    }

    SessionContext sqSession = connectionContext.getSession(browserInfo.getSessionId());

    if(sqSession == null){
      throw new IllegalStateException("No session for the producer connectionId "+browserInfo.getId());
    }

    SBProducerContext sbProducerContext = new SBProducerContext(browserInfo);
    sbProducerContext.setSessionContext(sqSession);
    sqSession.getConsumers().putIfAbsent(browserInfo.getId(), browserInfo);
    if(browserInfo != null){
      try {
        connectionContext.getBroker().addBrowser(browserInfo, sqSession);
      }
      catch (IOException e) {
        e.printStackTrace();
      }
      catch (JMSException e) {
        e.printStackTrace();
      }
      // handle duplicate
    }
  }

}
