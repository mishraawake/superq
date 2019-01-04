package org.apache.superq.network;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Consumer;

import javax.jms.JMSException;

import org.apache.superq.ConsumerInfo;
import org.apache.superq.SessionInfo;
import org.apache.superq.incoming.SBProducerContext;
import org.apache.superq.outgoing.SBConsumer;
import org.apache.superq.storage.TransactionalStore;
import org.apache.superq.storage.TransactionalStoreImpl;

public class SessionContext {

  private long transactionId = -1;
  private SessionInfo sessionInfo;
  private Map<Long, SBProducerContext> producers = new ConcurrentHashMap<>();
  private Map<Long, ConsumerInfo> consumers = new ConcurrentHashMap<>();;
  private ConnectionContext connectionContext;
  private TransactionalStore transactionalStore;

  public SessionContext(SessionInfo sessionInfo){
    this.sessionInfo = sessionInfo;
  }

  public long getTransactionId() {
    return transactionId;
  }

  public void setTransactionId(long transactionId) {
    this.transactionId = transactionId;
  }

  public SessionInfo getSessionInfo() {
    return sessionInfo;
  }

  public void setSessionInfo(SessionInfo sessionInfo) {
    this.sessionInfo = sessionInfo;
  }

  public Map<Long, SBProducerContext> getProducers() {
    return producers;
  }

  public void setProducers(Map<Long, SBProducerContext> producers) {
    this.producers = producers;
  }

  public Map<Long, ConsumerInfo> getConsumers() {
    return consumers;
  }

  public void setConsumers(Map<Long, ConsumerInfo> consumers) {
    this.consumers = consumers;
  }

  public ConnectionContext getConnectionContext() {
    return connectionContext;
  }

  public void setConnectionContext(ConnectionContext connectionContext) {
    this.connectionContext = connectionContext;
  }

  public boolean isTransactional() {
    return transactionId != -1;
  }

  public void commitTransaction() throws IOException, JMSException {
    transactionalStore.commit();
    transactionalStore = null;
    transactionId = -1;
  }

  public void rollbackTransaction() throws IOException, JMSException {
    transactionalStore.rollback();
    transactionalStore = null;
    transactionId = -1;
  }

  public void startTransaction(long transactionId){
    if(transactionalStore == null)
      this.transactionId = transactionId;
      transactionalStore = new TransactionalStoreImpl();
  }

  public TransactionalStore getCurrentTransaction(){
    return transactionalStore;
  }

  public void close() throws IOException {
    producers.clear();
    for(Map.Entry<Long, ConsumerInfo> consumerInfoEntry : consumers.entrySet()){
        this.getConnectionContext().getBroker().removeConsumer(consumerInfoEntry.getValue());
    }
  }
}
