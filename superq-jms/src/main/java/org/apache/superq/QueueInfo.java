package org.apache.superq;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.Objects;
import javax.jms.JMSException;
import javax.jms.Queue;

public class QueueInfo extends CorrelatedPacket implements Queue, SMQDestination {

  String queueName;
  private int id;

  @Override
  public void serializeFields(DataOutputStream dos) throws IOException {
    super.serializeFields(dos);
    serializeByteString(dos, queueName);
    serializeInt(dos, id);
  }

  @Override
  public void deSerializeFields(DataInputStream dis) throws IOException {
    super.deSerializeFields(dis);
    this.queueName = deserializeByteString(dis);
    this.id = deSerializeInteger(dis);
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

  public int getId() {
    return id;
  }

  public void setId(int id) {
    this.id = id;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o)
      return true;
    if (o == null || getClass() != o.getClass())
      return false;
    QueueInfo queueInfo = (QueueInfo) o;
    return getPacketId() == queueInfo.getPacketId() && id == queueInfo.id &&
            Objects.equals(queueName, queueInfo.queueName);
  }

  @Override
  public int hashCode() {
    return Objects.hash(getPacketId(), queueName, id);
  }

  @Override
  public String toString() {
    return "QueueInfo{" +
            "queueName='" + queueName + '\'' +
            ", id=" + id +
            ", packatId=" + getPacketId() +
            '}';
  }

  @Override
  public int getDestinationId() {
    return id;
  }
}
