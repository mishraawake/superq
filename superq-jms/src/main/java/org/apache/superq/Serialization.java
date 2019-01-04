package org.apache.superq;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

public interface Serialization {
  default int getFieldsSize(){
    return 16;
  }
  int getSize() throws IOException;
  byte[] getBuffer() throws IOException ;
  void acceptByteBuffer(byte[] bb) throws IOException;
  default short getType(){
    return 0;
  }
  void serializeFields(DataOutputStream dos) throws IOException;
  void deSerializeFields(DataInputStream dis) throws IOException;
}