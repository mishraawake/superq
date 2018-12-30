package org.apache.superq.reqres;

import org.apache.superq.ConnectionInfo;
import org.apache.superq.Serialization;
import org.apache.superq.network.ConnectionContext;

public class ConnectionInfoHandler implements RequestHandler<ConnectionInfo> {

  @Override
  public void handle(ConnectionInfo info, ConnectionContext connectionContext) {
    connectionContext.setInfo(info);
    // not an obvious handling
  }
}
