package org.apache.superq.storage;

import java.io.IOException;
import java.util.List;

import org.apache.superq.Serialization;
import sun.reflect.generics.reflectiveObjects.NotImplementedException;

public interface MessageStore<M extends Serialization> extends  MessageSupplier<M> {

  default void removeMessage(long messageId) throws IOException {
    throw new NotImplementedException();
  }

  default List<M> allMessages() throws IOException {
    throw new NotImplementedException();
  }

  default MessageEnumerator<M> getMessageEnumerator() {
    throw new NotImplementedException();
  }
}
