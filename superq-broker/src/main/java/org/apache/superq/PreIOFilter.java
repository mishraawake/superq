package org.apache.superq;

public class PreIOFilter {

  Broker broker;

  public PreIOFilter(Broker broker){
    this.broker = broker;
  }

  public void beforeIO(){
    if(broker.isCallbackThread()){
//      throw new RuntimeException("IO can not be called in callback thread");
    }
  }
}
