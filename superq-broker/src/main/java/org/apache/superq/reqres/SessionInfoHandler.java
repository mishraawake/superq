package org.apache.superq.reqres;

import org.apache.superq.Serialization;
import org.apache.superq.SessionInfo;
import org.apache.superq.network.ConnectionContext;

public class SessionInfoHandler implements RequestHandler<SessionInfo> {


  public SessionInfoHandler(){

  }
  @Override
  public void handle(SessionInfo sessionInfo, ConnectionContext connectionContext) {
    if(connectionContext.getInfo().getConnectionId() != sessionInfo.getConnectionId()){
      // error condition
      throw new IllegalStateException("Session connectionId "+sessionInfo.getConnectionId()+
                                              " does not match with actual connectionID " +
                                              + connectionContext.getInfo().getConnectionId());
    }
    if(connectionContext.getSession(sessionInfo.getSessionId()) == null) {
      connectionContext.registerSession(sessionInfo);
    }
  }
}
