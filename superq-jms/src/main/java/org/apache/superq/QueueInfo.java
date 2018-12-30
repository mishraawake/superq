package org.apache.superq;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import javax.jms.JMSException;
import javax.jms.Queue;

public class QueueInfo extends CorrelatedPacket implements Queue {

  String queueName;
  private long id;

  @Override
  public void serializeFields(DataOutputStream dos) throws IOException {
    super.serializeFields(dos);
    serializeByteString(dos, queueName);
    serializeLong(dos, id);
  }

  @Override
  public void deSerializeFields(DataInputStream dis) throws IOException {
    super.deSerializeFields(dis);
    this.queueName = deserializeByteString(dis);
    this.id = deSerializeLong(dis);
  }

  public void setQueueName(String queueName) {
    this.queueName = queueName;
  }

  public short getType(){
    return 7;
  }

  @Override
  public String getQueueName() throws JMSException {
    return queueName;
  }

  public long getId() {
    return id;
  }

  public void setId(long id) {
    this.id = id;
  }
}
