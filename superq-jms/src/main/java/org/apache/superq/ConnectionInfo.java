package org.apache.superq;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

public class ConnectionInfo extends SerializationSupport {

  private long connectionId;

  @Override
  public void serializeFields(DataOutputStream dos) throws IOException {
    serializeLong(dos, connectionId);
  }

  @Override
  public void deSerializeFields(DataInputStream dis) throws IOException {
    connectionId = deSerializeLong(dis);
  }

  public short getType(){
    return 1;
  }
}
