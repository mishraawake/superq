package org.apache.superq.storage;

import java.io.IOException;
import javax.jms.JMSException;

import org.apache.superq.CommitTransaction;
import org.apache.superq.SMQMessage;
import org.apache.superq.network.ConnectionContext;

public interface TransactionalStore extends MessageStore<SMQMessage> {
  void addSynchrounization(TransactionSync sync);
  void commit(ConnectionContext connectionContext, CommitTransaction commitTransaction) throws JMSException, IOException;
  void rollback();
}
