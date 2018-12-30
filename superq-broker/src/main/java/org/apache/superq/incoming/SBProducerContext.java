package org.apache.superq.incoming;

import org.apache.superq.ProducerInfo;
import org.apache.superq.network.ConnectionContext;
import org.apache.superq.network.SessionContext;

public class SBProducerContext {
  private ConnectionContext connectionContext;
  private SessionContext sessionContext;
  private ProducerInfo producerInfo;
  private long messageSeqId;
  private long totalMessageReceived;

  public SBProducerContext(ProducerInfo producerInfo){
    this.producerInfo = producerInfo;
  }
  public ConnectionContext getConnectionContext(){
     return this.connectionContext;
   }

  public void setConnectionContext(ConnectionContext connectionContext){
      this.connectionContext = connectionContext;
  }

  public SessionContext getSessionContext() {
    return sessionContext;
  }

  public void setSessionContext(SessionContext sessionContext) {
    this.sessionContext = sessionContext;
  }


  public long getMessageSeqId() {
    return messageSeqId;
  }

  public void setMessageSeqId(long messageSeqId) {
    this.messageSeqId = messageSeqId;
  }

  public long getTotalMessageReceived() {
    return totalMessageReceived;
  }

  public void setTotalMessageReceived(long totalMessageReceived) {
    this.totalMessageReceived = totalMessageReceived;
  }
}
