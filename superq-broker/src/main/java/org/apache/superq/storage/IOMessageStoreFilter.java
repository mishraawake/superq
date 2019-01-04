package org.apache.superq.storage;

import java.io.IOException;

import org.apache.superq.Broker;
import org.apache.superq.PreIOFilter;
import org.apache.superq.SMQMessage;

public class IOMessageStoreFilter implements MessageStoreFilter {

  MessageStore store;
  PreIOFilter preIOFilter;

  public IOMessageStoreFilter(MessageStore store, Broker broker){
    this.store = store;
    preIOFilter = new PreIOFilter(broker);
  }

  @Override
  public SMQMessage getNextMessage() throws IOException {
    preIOFilter.beforeIO();
    return store.getNextMessage();
  }


  @Override
  public boolean hasMoreMessage() throws IOException {
    preIOFilter.beforeIO();
    return store.hasMoreMessage();
  }

  @Override
  public void addMessage(SMQMessage smqMessage) throws IOException {
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
}
