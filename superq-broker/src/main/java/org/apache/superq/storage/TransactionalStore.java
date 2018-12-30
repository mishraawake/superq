package org.apache.superq.storage;

import java.io.IOException;
import javax.jms.JMSException;

public interface TransactionalStore extends MessageStore {
  void addSynchrounization(TransactionSync sync);
  void commit() throws JMSException, IOException;
  void rollback();
}
