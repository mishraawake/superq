package org.apache.superq;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

public class ProduceAck extends SerializationSupport{
  private long sessionId;
  private long producerId;
  private long messageId;

  public long getSessionId() {
    return sessionId;
  }

  public void setSessionId(long sessionId) {
    this.sessionId = sessionId;
  }

  public long getProducerId() {
    return producerId;
  }

  public void setProducerId(long producerId) {
    this.producerId = producerId;
  }

  public long getMessageId() {
    return messageId;
  }

  public void setMessageId(long messageId) {
    this.messageId = messageId;
  }

  @Override
  public void serializeFields(DataOutputStream dos) throws IOException {
    super.serializeFields(dos);
    serializeLong(dos, sessionId);
    serializeLong(dos, producerId);
    serializeLong(dos, messageId);
  }

  @Override
  public void deSerializeFields(DataInputStream dis) throws IOException {
    super.deSerializeFields(dis);
    sessionId = deSerializeLong(dis);
    producerId = deSerializeLong(dis);
    messageId = deSerializeLong(dis);
  }

  public short getType(){
    return 6;
  }
}
