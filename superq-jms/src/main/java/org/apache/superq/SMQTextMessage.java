package org.apache.superq;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.Objects;
import javax.jms.JMSException;
import javax.jms.TextMessage;

public class SMQTextMessage extends SMQMessage implements TextMessage {

  String content;

  @Override
  public void setText(String content) throws JMSException {
    this.content = content;
  }

  @Override
  public String getText() throws JMSException {
    return this.content;
  }

  @Override
  public short getType() {
    return 0;
  }

  @Override
  public void serializeFields(DataOutputStream dos) throws IOException {
    super.serializeFields(dos);
    serializeIntString(dos, content);
  }

  @Override
  public void deSerializeFields(DataInputStream dis) throws IOException {
    super.deSerializeFields(dis);
    content =  deserializeIntString(dis);
  }

  @Override
  public String toString() {
    return "SMQTextMessage{" +
            "content='" + content + '\'' +
            super.toString() +
            '}';
  }

  @Override
  public boolean equals(Object o) {
    if (this == o)
      return true;
    if (o == null || getClass() != o.getClass())
      return false;
    if (!super.equals(o))
      return false;
    SMQTextMessage that = (SMQTextMessage) o;
    return Objects.equals(content, that.content);
  }

  @Override
  public int hashCode() {
    return Objects.hash(super.hashCode(), content);
  }
}
