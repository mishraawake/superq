package org.apache.superq;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

public class ConsumerInfo extends SerializationSupport {
  @Override
  public void serializeFields(DataOutputStream dos) throws IOException {

  }

  @Override
  public void deSerializeFields(DataInputStream dis) throws IOException {

  }

  public short getType(){
    return 9;
  }
}
