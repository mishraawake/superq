package org.apache.superq.storage;


import java.io.IOException;

import org.apache.superq.SMQMessage;
import sun.reflect.generics.reflectiveObjects.NotImplementedException;

public interface MessageSupplier {
  SMQMessage getNextMessage() throws IOException;
  default MessageEnumerator browserEnumerator() throws IOException{
    throw new NotImplementedException();
  }
  default boolean hasMoreMessage() throws IOException {
    throw new NotImplementedException();
  }
  void addMessage(SMQMessage smqMessage) throws IOException;
  boolean doesSpaceLeft();
}
