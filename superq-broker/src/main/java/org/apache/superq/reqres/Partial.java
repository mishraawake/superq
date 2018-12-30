package org.apache.superq.reqres;

import java.io.IOException;
import java.nio.channels.SocketChannel;

import org.apache.superq.Task;
import org.apache.superq.network.ConnectionContext;


public interface Partial {
  void tryComplete(SocketChannel ssc) throws IOException ;
  boolean complete();
  Task handle(ConnectionContext context) throws IOException;
}
