package org.apache.superq.storage;


import java.io.IOException;

import org.apache.superq.SMQMessage;

public interface MessageSupplier {
  SMQMessage getNextMessage() throws IOException;
  boolean hasMoreMessage();
  void addMessage(SMQMessage smqMessage) throws IOException;
  boolean doesSpaceLeft();
}
