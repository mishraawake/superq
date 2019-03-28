package org.apache.superq;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.Objects;

public class CorrelatedPacket extends SerializationSupport{

  private long packetId;
  private long debugTime;

  public long getPacketId() {
    return packetId;
  }

  public void setPacketId(long packetId) {
    this.packetId = packetId;
  }

  public long getDebugTime() {
    return debugTime;
  }

  public void setDebugTime(long debugTime) {
    this.debugTime = debugTime;
  }

  @Override
  public void serializeFields(DataOutputStream dos) throws IOException {
    super.serializeFields(dos);
    serializeLong(dos, packetId);
    serializeLong(dos, debugTime);
  }

  @Override
  public void deSerializeFields(DataInputStream dis) throws IOException {
    super.deSerializeFields(dis);
    packetId = deSerializeLong(dis);
    debugTime = deSerializeLong(dis);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o)
      return true;
    if (o == null || getClass() != o.getClass())
      return false;
    CorrelatedPacket that = (CorrelatedPacket) o;
    return packetId == that.packetId;
  }

  @Override
  public int hashCode() {
    return Objects.hash(packetId);
  }

  @Override
  public String toString() {
    return "CorrelatedPacket{" +
            "packetId=" + packetId +
            '}';
  }
}
