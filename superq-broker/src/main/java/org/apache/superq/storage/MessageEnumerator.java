package org.apache.superq.storage;

import java.io.IOException;

import org.apache.superq.SMQMessage;

public interface MessageEnumerator {
  public boolean hasMoreElements() throws IOException;
  public SMQMessage nextElement();
}
