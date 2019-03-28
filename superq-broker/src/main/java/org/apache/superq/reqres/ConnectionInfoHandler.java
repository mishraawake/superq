package org.apache.superq.reqres;

import java.io.IOException;

import org.apache.superq.ConnectionInfo;
import org.apache.superq.Serialization;
import org.apache.superq.network.ConnectionContext;

public class ConnectionInfoHandler implements RequestHandler<ConnectionInfo> {

  @Override
  public void handle(ConnectionInfo info, ConnectionContext connectionContext) {
    long stime = System.currentTimeMillis();
    //System.out.println(info);
    connectionContext.setInfo(info);
    if(System.currentTimeMillis() - info.getDebugTime()  >= 0){
      System.out.println("Debug time = "+( System.currentTimeMillis() - info.getDebugTime()));
     // info.setDebugTime(System.currentTimeMillis());
    }
    try {
      connectionContext.sendAsyncPacket(info);
    }
    catch (IOException e) {
      e.printStackTrace();
    }
    // not an obvious handling
  }
}
