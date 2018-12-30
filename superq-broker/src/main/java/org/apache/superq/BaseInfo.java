package org.apache.superq;

import java.nio.ByteBuffer;

import org.apache.superq.db.Sizeable;

public class BaseInfo implements Sizeable {
  @Override
  public int getSize() {
    return 0;
  }

  @Override
  public ByteBuffer getBuffer() {
    return null;
  }

  @Override
  public void acceptByteBuffer(ByteBuffer bb) {

  }
}
