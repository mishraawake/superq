package org.apache.superq;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.Arrays;

public class ConsumerInfo extends ProducerInfo {

  int qid;
  boolean isAsync;

  @Override
  public void serializeFields(DataOutputStream dos) throws IOException {
    super.serializeFields(dos);
    serializeInt(dos, qid);
    serializeBoolean(dos, isAsync);
  }

  @Override
  public void deSerializeFields(DataInputStream dis) throws IOException {
    super.deSerializeFields(dis);
    qid = deSerializeInteger(dis);
    isAsync = deSerializeBoolean(dis);

  }

  public int getQid() {
    return qid;
  }

  public void setQid(int qid) {
    this.qid = qid;
  }

  public short getType(){
    return 9;
  }

  public boolean isAsync() {
    return isAsync;
  }

  public void setAsync(boolean async) {
    isAsync = async;
  }

  @Override
  public String toString() {
    return "ConsumerInfo{" +
            "qid=" + qid +
            ", isAsync=" + isAsync +
            ", fieldArray=" + fieldArray +
            ", deserializingIndex=" + deserializingIndex +
            ", bytes=" + Arrays.toString(bytes) +
            '}';
  }
}
