package org.apache.superq.storage;

import org.apache.superq.SMQMessage;

public class QueueStats implements QueueMXBean {

  SBQueue<SMQMessage> messageQueue;

  public QueueStats(SBQueue<SMQMessage> messageQueue){
    this.messageQueue = messageQueue;
  }

  @Override
  public int getNumberOfConsumers() {
    return messageQueue.getConsumers().size();
  }

  @Override
  public boolean isMoreDataPresent() {
    return messageQueue.getStatus().getMessageEnumerator().moreMessage();
  }

  @Override
  public boolean isLoading() {
    return messageQueue.getStatus().getMessageEnumerator().beingLoaded();
  }

  @Override
  public int getMemorySize() {
    return messageQueue.getStatus().getMessageEnumerator().memorySize();
  }
}
