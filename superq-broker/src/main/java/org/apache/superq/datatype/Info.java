package org.apache.superq.datatype;

import java.nio.ByteBuffer;

import org.apache.superq.db.Sizeable;

public class Info implements Sizeable {

  private PeerId consumerId;
  // in form of <ip>, <port>

  private String queueName;

  private short queueNameLength;

  public PeerId getConsumerId() {
    return consumerId;
  }

  public void setConsumerId(PeerId consumerId) {
    this.consumerId = consumerId;
  }

  public String getQueueName() {
    return queueName;
  }

  public void setQueueName(String queueName) {
    this.queueName = queueName;
  }

  public short getQueueNameLength() {
    return queueNameLength;
  }

  public void setQueueNameLength(short queueNameLength) {
    this.queueNameLength = queueNameLength;
  }

  @Override
  public int getSize() {
    return  queueNameLength + Short.BYTES;
  }

  @Override
  public ByteBuffer getBuffer() {
     ByteBuffer bb = ByteBuffer.allocate(getSize());

     bb.putInt(consumerId.getPort());
     bb.put(consumerId.getIplength());
     bb.put(consumerId.getIp().getBytes());

     bb.putShort(queueNameLength);
     bb.put(queueName.getBytes());
     return bb;
  }

  @Override
  public void acceptByteBuffer(ByteBuffer bb) {
    consumerId = new PeerId();
    consumerId.setPort(bb.getInt());
    byte ipLength = bb.get();
    consumerId.setIplength(ipLength);
    byte[] ip = new byte[ipLength];
    consumerId.setIp(new String(ip));

    this.queueNameLength = bb.getShort();
    byte[] name = new byte[queueNameLength];
    bb.get(name);
    this.queueName = new String(name);
  }



  public long getLongId(){
    return consumerId.hashCode();
  }

  @Override
  public String toString() {
    return "ConsumerInfo{" +
            "consumerId=" + consumerId +
            ", queueName='" + queueName + '\'' +
            ", queueNameLength=" + queueNameLength +
            '}';
  }

}
