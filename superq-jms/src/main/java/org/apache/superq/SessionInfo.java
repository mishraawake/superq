package org.apache.superq;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

public class SessionInfo extends CorrelatedPacket {

  long sessionId;
  long connectionId;

  public void serializeFields(DataOutputStream dos) throws IOException {
    super.serializeFields(dos);
    serializeLong(dos, sessionId);
    serializeLong(dos, connectionId);
  }

  @Override
  public void deSerializeFields(DataInputStream dis) throws IOException {
    super.deSerializeFields(dis);
    sessionId = deSerializeLong(dis);
    connectionId = deSerializeLong(dis);
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

  @Override
  public short getType(){
    return 8;
  }
}
