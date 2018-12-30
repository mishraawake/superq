package org.apache.superq;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
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
    serializeIntString(dos, content);
    super.serializeFields(dos);
  }

  @Override
  public void deSerializeFields(DataInputStream dis) throws IOException {
    content =  deserializeIntString(dis);
    super.deSerializeFields(dis);
  }
}
