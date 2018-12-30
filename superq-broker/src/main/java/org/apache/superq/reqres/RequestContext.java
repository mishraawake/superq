package org.apache.superq.reqres;

import java.nio.channels.SocketChannel;

public class RequestContext {

  String connectionId;
  SocketChannel sc;

  public String getConnectionId() {
    return connectionId;
  }

  public void setConnectionId(String connectionId) {
    this.connectionId = connectionId;
  }

  public SocketChannel getSc() {
    return sc;
  }

  public void setSc(SocketChannel sc) {
    this.sc = sc;
  }
}
