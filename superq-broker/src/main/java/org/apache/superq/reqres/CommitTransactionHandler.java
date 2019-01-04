package org.apache.superq.reqres;

import java.io.IOException;
import javax.jms.JMSException;

import org.apache.superq.CommitTransaction;
import org.apache.superq.network.ConnectionContext;
import org.apache.superq.network.SessionContext;

public class CommitTransactionHandler implements RequestHandler<CommitTransaction> {

  public CommitTransactionHandler(){

  }
  @Override
  public void handle(CommitTransaction commitTransaction, ConnectionContext connectionContext) {
    SessionContext sessionContext = connectionContext.getSession(commitTransaction.getSessionId());
    if(sessionContext != null && sessionContext.getTransactionId() == commitTransaction.getTransactionId()){
      try {
        sessionContext.commitTransaction();
        connectionContext.sendAsyncPacket(commitTransaction);
      }
      catch (IOException e) {
        e.printStackTrace();
      }
      catch (JMSException e) {
        e.printStackTrace();
      }
    }
  }
}
