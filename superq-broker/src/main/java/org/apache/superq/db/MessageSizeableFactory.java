package org.apache.superq.db;

import org.apache.superq.SMQMessage;
import org.apache.superq.SMQTextMessage;
import org.apache.superq.Serialization;

public class MessageSizeableFactory<T extends Serialization> implements SizeableFactory<SMQMessage> {
  @Override
  public SMQMessage getSizeable() {
    return new SMQMessage(){};
  }

  @Override
  public SMQMessage getSizeableByType(short type) {
    if(type == 0)
      return new SMQTextMessage();
    return null;
  }
}
