package org.apache.superq.reqres;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;

public class MaxReaderPartialRequest extends PartialRequest {

  public void tryComplete(SocketChannel ssc) throws IOException {
    ByteBuffer bf = ByteBuffer.allocate(100000);
    int x = ssc.read(bf);
    System.out.println(x);
  }
}
