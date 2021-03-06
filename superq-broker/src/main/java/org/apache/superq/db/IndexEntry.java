package org.apache.superq.db;

import java.io.ByteArrayOutputStream;
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

  protected void initializeBytes() throws IOException {
    if(bytes == null) {
      ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
      DataOutputStream dos = new DataOutputStream(byteArrayOutputStream);
      serializeFields(dos);
      bytes = byteArrayOutputStream.toByteArray();
      //fillField(bytes);
    }
  }

  @Override
  public void serializeFields(DataOutputStream dos) throws IOException {
    //super.serializeFields(dos);
    doSerializeLong(dos, messageLocation);
    doSerializeInteger(dos, messageLength);
  }

  @Override
  public void deSerializeFields(DataInputStream dis) throws IOException {
   // super.deSerializeFields(dis);
    messageLocation = doGetLong(dis);
    messageLength = doGetInteger(dis);
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
