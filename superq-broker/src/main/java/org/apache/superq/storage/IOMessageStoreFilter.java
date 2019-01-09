package org.apache.superq.storage;

import java.io.IOException;
import java.util.List;

import org.apache.superq.Broker;
import org.apache.superq.PreIOFilter;
import org.apache.superq.Serialization;
import org.apache.superq.Task;

public class IOMessageStoreFilter<M extends Serialization> implements MessageStoreFilter<M> {

  MessageStore<M> store;
  PreIOFilter preIOFilter;

  public IOMessageStoreFilter(MessageStore<M> store, Broker broker){
    this.store = store;
    preIOFilter = new PreIOFilter(broker);
  }

  @Override
  public M getNextMessage() throws IOException {
    preIOFilter.beforeIO();
    return store.getNextMessage();
  }


  @Override
  public boolean hasMoreMessage(Task task) throws IOException {
    preIOFilter.beforeIO();
    return store.hasMoreMessage(task);
  }

  @Override
  public void addMessage(M smqMessage) throws IOException {
    preIOFilter.beforeIO();
    store.addMessage(smqMessage);
  }

  @Override
  public boolean doesSpaceLeft() {
    preIOFilter.beforeIO();
    return store.doesSpaceLeft();
  }

  @Override
  public void removeMessage(long messageId) throws IOException {
    preIOFilter.beforeIO();
    store.removeMessage(messageId);
  }

  @Override
  public List<M> allMessages() throws IOException {
    preIOFilter.beforeIO();
    return store.allMessages();
  }
}
