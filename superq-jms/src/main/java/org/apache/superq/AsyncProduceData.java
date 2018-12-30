package org.apache.superq;

import javax.jms.CompletionListener;
import javax.jms.Message;

public class AsyncProduceData {

  private CompletionListener completionListener;
  private Message  message;

  public CompletionListener getCompletionListener() {
    return completionListener;
  }

  public void setCompletionListener(CompletionListener completionListener) {
    this.completionListener = completionListener;
  }

  public Message getMessage() {
    return message;
  }

  public void setMessage(Message message) {
    this.message = message;
  }
}
