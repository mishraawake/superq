package org.apache.superq;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

public class ProducerInfo extends SerializationSupport{

  private long producerId;
  private long sessionId;
  private long connectionId;

  @Override
  public void serializeFields(DataOutputStream dos) throws IOException {
    serializeLong(dos, producerId);
    serializeLong(dos, sessionId);
    serializeLong(dos, connectionId);
  }

  @Override
  public void deSerializeFields(DataInputStream dis) throws IOException {
    producerId = deSerializeLong(dis);
    sessionId = deSerializeLong(dis);
    connectionId = deSerializeLong(dis);
  }

  public short getType(){
    return 2;
  }

  public long getProducerId() {
    return producerId;
  }

  public void setProducerId(long producerId) {
    this.producerId = producerId;
  }

  public long getSessionId() {
    return sessionId;
  }

  public void setSessionId(long sessionId) {
    this.sessionId = sessionId;
  }

  public long getConnectionId() {
    return connectionId;
  }

  public void setConnectionId(long connectionId) {
    this.connectionId = connectionId;
  }
}
