package org.apache.superq.reqres;

import org.apache.superq.StartTransaction;
import org.apache.superq.network.ConnectionContext;
import org.apache.superq.network.SessionContext;

public class StartTransactionHandler implements RequestHandler<StartTransaction> {

  public StartTransactionHandler(){

  }
  @Override
  public void handle(StartTransaction startTransaction, ConnectionContext connectionContext) {
    //System.out.println("start tr");
    SessionContext sessionContext = connectionContext.getSession(startTransaction.getSessionId());
    if(sessionContext != null){
      sessionContext.setTransactionId(startTransaction.getTransactionId());
     // System.out.println("start tr"+sessionContext.getTransactionId() + "  "+startTransaction.getTransactionId());
      sessionContext.startTransaction(startTransaction.getTransactionId());
    }
  }

}
