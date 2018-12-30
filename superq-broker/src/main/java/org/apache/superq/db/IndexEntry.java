package org.apache.superq.db;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Objects;

import org.apache.superq.Serialization;
import org.apache.superq.SerializationSupport;

public class IndexEntry extends SerializationSupport {
  private long messageLocation;
  private int messageLength;
  public static int SIZE  = 8*1 + 4;

  public long getMessageLocation() {
    return messageLocation;
  }

  public void setMessageLocation(long messageLocation) {
    this.messageLocation = messageLocation;
  }

  public int getMessageLength() {
    return messageLength;
  }

  public void setMessageLength(int messageLength) {
    this.messageLength = messageLength;
  }


  @Override
  public void serializeFields(DataOutputStream dos) throws IOException {
    serializeLong(dos, messageLocation);
    serializeInt(dos, messageLength);
  }

  @Override
  public void deSerializeFields(DataInputStream dis) throws IOException {
    messageLocation = deSerializeLong(dis);
    messageLength = deSerializeInteger(dis);
  }

  @Override
  public int getSize() {
    return SIZE;
  }

  @Override
  public String toString() {
    return "IndexEntry{" +
            ", messageLocation=" + messageLocation +
            ", messageLength=" + messageLength +
            '}';
  }

  @Override
  public boolean equals(Object o) {
    if (this == o)
      return true;
    if (o == null || getClass() != o.getClass())
      return false;
    IndexEntry that = (IndexEntry) o;
    return messageLocation == that.messageLocation &&
            messageLength == that.messageLength;
  }

  @Override
  public int hashCode() {
    return Objects.hash(messageLocation, messageLength);
  }

}
