package org.apache.superq.reqres;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;

import org.apache.superq.Serialization;
import org.apache.superq.Task;
import org.apache.superq.network.ConnectionContext;

public class ResponsePartial implements Partial {

  Serialization serialization;
  volatile boolean started = false;
  int remaining ;
  ByteBuffer bb;

  public ResponsePartial(Serialization serialization){
    this.serialization = serialization;
  }

  @Override
  public void tryComplete(SocketChannel ssc) throws IOException {
    byte[] responseByte = serialization.getBuffer();
    if(!started){
      started = true;
      remaining = responseByte.length + Integer.BYTES + Short.BYTES;
      bb = ByteBuffer.allocate(remaining);
      bb.putInt(serialization.getSize());
      bb.putShort(serialization.getType());
      bb.put(responseByte);
      bb.flip();
      remaining -= ssc.write(bb);
      return;
    }
    if(remaining > 0){
      remaining -= ssc.write(bb);
    }
  }

  @Override
  public boolean complete() {
    return remaining == 0;
  }

  @Override
  public Task handle(ConnectionContext cc) throws IOException {
    return null;
  }

  public Serialization getSerialization() {
    return serialization;
  }

  public boolean isStarted() {
    return started;
  }

  public void setStarted(boolean started) {
    this.started = started;
  }
}
