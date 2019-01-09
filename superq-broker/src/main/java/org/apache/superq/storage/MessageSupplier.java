package org.apache.superq.storage;


import java.io.IOException;
import java.util.List;

import org.apache.superq.SMQMessage;
import org.apache.superq.Serialization;
import org.apache.superq.Task;
import sun.reflect.generics.reflectiveObjects.NotImplementedException;

public interface MessageSupplier<M extends Serialization> {
  M getNextMessage() throws IOException;
  default MessageEnumerator<M> browserEnumerator(Class<M> className) throws IOException{
    throw new NotImplementedException();
  }
  default boolean hasMoreMessage(Task afterIO) throws IOException {
    throw new NotImplementedException();
  }
  void addMessage(M smqMessage) throws IOException;
  boolean doesSpaceLeft();
}
