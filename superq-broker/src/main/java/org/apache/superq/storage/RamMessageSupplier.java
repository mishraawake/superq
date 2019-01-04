package org.apache.superq.storage;

import java.io.IOException;
import java.util.concurrent.BlockingDeque;
import java.util.concurrent.LinkedBlockingDeque;

import org.apache.superq.SMQMessage;

public class RamMessageSupplier implements MessageSupplier {

  MessageStore store;
  String qname;
  int spaceAllocated;


  BlockingDeque<SMQMessage> messageQueue = new LinkedBlockingDeque<>();

  public RamMessageSupplier(MessageStore store, String qname){
    this.store = store;
    this.qname = qname;
  }


  @Override
  public SMQMessage getNextMessage() throws IOException {
    if(messageQueue.size() > 0){
      return messageQueue.poll();
    }
    return null;
  }

  @Override
  public MessageEnumerator browserEnumerator() throws IOException {
    return null;
  }


  @Override
  public void addMessage(SMQMessage smqMessage) throws IOException {
    messageQueue.add(smqMessage);
  }

  @Override
  public boolean doesSpaceLeft() {
    return false;
  }
}
