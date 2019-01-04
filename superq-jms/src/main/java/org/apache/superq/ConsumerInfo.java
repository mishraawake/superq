package org.apache.superq;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

public class ConsumerInfo extends ProducerInfo {

  int qid;

  @Override
  public void serializeFields(DataOutputStream dos) throws IOException {
    super.serializeFields(dos);
    serializeInt(dos, qid);
  }

  @Override
  public void deSerializeFields(DataInputStream dis) throws IOException {
    super.deSerializeFields(dis);
    qid = deSerializeInteger(dis);
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
}
