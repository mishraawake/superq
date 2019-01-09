package org.apache.superq.storage;

import java.io.IOException;

import org.apache.superq.Serialization;
import org.apache.superq.Task;

public interface MessageEnumerator<M extends Serialization> {
  public boolean hasMoreElements(Task task) throws IOException;
  public M nextElement();
}
