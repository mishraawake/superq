package org.apache.superq;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

public class CorrelatedPacket extends SerializationSupport{

  private long packetId;

  public long getPacketId() {
    return packetId;
  }

  public void setPacketId(long packetId) {
    this.packetId = packetId;
  }

  @Override
  public void serializeFields(DataOutputStream dos) throws IOException {
    serializeLong(dos, packetId);
  }

  @Override
  public void deSerializeFields(DataInputStream dis) throws IOException {
    packetId = deSerializeLong(dis);
  }
}
