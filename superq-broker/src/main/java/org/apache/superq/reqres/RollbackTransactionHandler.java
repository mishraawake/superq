package org.apache.superq.reqres;

import java.io.IOException;
import javax.jms.JMSException;

import org.apache.superq.RollbackTransaction;
import org.apache.superq.network.ConnectionContext;
import org.apache.superq.network.SessionContext;

public class RollbackTransactionHandler implements RequestHandler<RollbackTransaction> {

  public RollbackTransactionHandler(){

  }

  @Override
  public void handle(RollbackTransaction rollbackTransaction, ConnectionContext connectionContext) {
    SessionContext sessionContext = connectionContext.getSession(rollbackTransaction.getSessionId());
    if(sessionContext != null && sessionContext.getTransactionId() == rollbackTransaction.getTransactionId()){
      try {
        sessionContext.rollbackTransaction();
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
