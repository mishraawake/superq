package org.apache.superq;

import java.nio.ByteBuffer;

import org.apache.superq.db.Sizeable;

public class AckInfo implements Sizeable {

  private long messageId;

  public long getMessageId() {
    return messageId;
  }

  public void setMessageId(long messageId) {
    this.messageId = messageId;
  }

  @Override
  public int getSize() {
    return Long.BYTES;
  }

  @Override
  public ByteBuffer getBuffer() {
    ByteBuffer bb = ByteBuffer.allocate(8);
    bb.putLong(messageId);
    return bb;
  }

  @Override
  public ByteBuffer getBufferWithType() {
    ByteBuffer bb = ByteBuffer.allocate(8 + Short.BYTES);
    bb.putShort(getType());
    bb.putLong(messageId);
    return bb;
  }

  @Override
  public void acceptByteBuffer(ByteBuffer bb) {
    messageId = bb.getLong();
  }

  @Override
  public short getType() {
    return 3;
  }
}
