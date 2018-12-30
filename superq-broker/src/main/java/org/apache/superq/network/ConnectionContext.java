package org.apache.superq.network;


import java.io.IOException;
import java.nio.channels.SelectionKey;
import java.nio.channels.SocketChannel;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.superq.Broker;
import org.apache.superq.ConnectionInfo;
import org.apache.superq.Serialization;
import org.apache.superq.SessionInfo;
import org.apache.superq.Task;
import org.apache.superq.reqres.PartialRequest;
import org.apache.superq.reqres.ResponsePartial;

public class ConnectionContext {

  Queue<ResponsePartial> queuedUpResponses = new LinkedBlockingQueue<>();
  private Map<Long, SessionContext> sessions = new ConcurrentHashMap<>();

  SocketChannel sc;
  SelectionKey key;
  AtomicLong totalResponse;
  ConnectionInfo info;
  PartialRequest pr;
  Broker broker;

  public ConnectionContext(SocketChannel sc, SelectionKey key, Broker broker){
    this.sc = sc;
    this.key = key;
    this.broker = broker;
  }

  // will be executed by callback thread
  public synchronized void sendAsyncPacket(Serialization serialization){
    queuedUpResponses.add(new ResponsePartial(serialization));
    key.interestOps(SelectionKey.OP_WRITE | SelectionKey.OP_READ);
  }

  // will be executed by request/response thread.
  void writeResponse(){
    ResponsePartial pr = queuedUpResponses.peek();
    if(pr != null){
      try {
        pr.tryComplete(sc);
      } catch (IOException ioe){
        closeConnection();
      }
      if (pr.complete()) {
        completeResponse();
      }
    }
  }

  private synchronized void completeResponse() {
    totalResponse.incrementAndGet();
    queuedUpResponses.poll();
    if(queuedUpResponses.isEmpty()){
      key.interestOps(SelectionKey.OP_READ);
    }
  }


  void readRequest(){
    if(pr != null){
      try {
        pr.tryComplete(sc);
        if(pr.complete()){
          Task handleTask = pr.handle(this);
          broker.putOnCallback(handleTask);
          pr = new PartialRequest();
        }
      } catch (IOException io){
        closeConnection();
      }
    }
  }

  private void closeConnection(){

  }

  public void setInfo(ConnectionInfo info){
    this.info = info;
  }

  public ConnectionInfo getInfo(){
    return this.info;
  }

  public Broker getBroker() {
    return broker;
  }

  public void setBroker(Broker broker) {
    this.broker = broker;
  }

  public SessionContext getSession(long sessionId){
    return sessions.get(sessionId);
  }

  public void registerSession(SessionInfo sessionInfo){
    sessions.putIfAbsent(sessionInfo.getSessionId(), new SessionContext(sessionInfo));
  }
}
