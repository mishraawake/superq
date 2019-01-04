package org.apache.superq.storage;

import java.io.IOException;

import sun.reflect.generics.reflectiveObjects.NotImplementedException;

public interface MessageStore extends  MessageSupplier {

  default void removeMessage(long messageId) throws IOException {
    throw new NotImplementedException();
  }
}
