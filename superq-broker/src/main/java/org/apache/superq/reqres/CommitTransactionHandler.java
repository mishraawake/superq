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
    //System.out.println("Commit info received "+System.currentTimeMillis());
    if(System.currentTimeMillis() - commitTransaction.getDebugTime()  > 10){
      //System.out.println("Debug time commit = "+( System.currentTimeMillis() - commitTransaction.getDebugTime()));
      // info.setDebugTime(System.currentTimeMillis());
    }
    SessionContext sessionContext = connectionContext.getSession(commitTransaction.getSessionId());
    System.out.println(sessionContext.getTransactionId() + "  "+commitTransaction.getTransactionId());
    if(sessionContext != null && sessionContext.getTransactionId() == commitTransaction.getTransactionId()){
      try {
        sessionContext.commitTransaction(commitTransaction);

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
