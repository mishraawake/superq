package org.apache.superq.db;

import java.nio.ByteBuffer;

public interface Sizeable {
  int getSize();
  ByteBuffer getBuffer();
  default ByteBuffer getBufferWithType(){
    return getBuffer();
  }
  void acceptByteBuffer(ByteBuffer bb);
  default short getType(){
    return 0;
  }
}
