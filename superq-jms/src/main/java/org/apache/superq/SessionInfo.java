package org.apache.superq;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.Objects;

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

  @Override
  public boolean equals(Object o) {
    if (this == o)
      return true;
    if (o == null || getClass() != o.getClass())
      return false;
    SessionInfo that = (SessionInfo) o;
    return sessionId == that.sessionId &&
            connectionId == that.connectionId;
  }

  @Override
  public int hashCode() {
    return Objects.hash(sessionId, connectionId);
  }

  @Override
  public String toString() {
    return "SessionInfo{" +
            "sessionId=" + sessionId +
            ", connectionId=" + connectionId +
            '}';
  }
}
