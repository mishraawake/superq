package org.apache.superq.network;


import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.channels.SelectionKey;
import java.nio.channels.SocketChannel;
import java.util.Iterator;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.superq.Broker;
import org.apache.superq.ConnectionInfo;
import org.apache.superq.JumboText;
import org.apache.superq.SMQMessage;
import org.apache.superq.Serialization;
import org.apache.superq.SessionInfo;
import org.apache.superq.Task;
import org.apache.superq.reqres.MaxReaderPartialRequest;
import org.apache.superq.reqres.PartialRequest;
import org.apache.superq.reqres.ResponsePartial;

public class ConnectionContext {

  Queue<Serialization> queuedUpResponses = new LinkedBlockingQueue<>();
  private Map<Long, SessionContext> sessions = new ConcurrentHashMap<>();

  SocketChannel sc;
  SelectionKey key;
  AtomicLong totalResponse = new AtomicLong(0);
  ConnectionInfo info;
  PartialRequest pr = new PartialRequest();
  Broker broker;
  String id;
  private volatile boolean goForJumbo = true;
  private volatile ResponsePartial responsePartial;

  public ConnectionContext(SocketChannel sc, SelectionKey key, Broker broker, String id){
    this.sc = sc;
    this.key = key;
    this.broker = broker;
    this.id = id;
  }

  // will be executed by callback thread
  public synchronized void sendAsyncPacket(Serialization serialization) throws IOException {
    queuedUpResponses.add((serialization));
    if(goForJumbo){
      goForJumbo = false;
      if(queuedUpResponses.size() > 1){
        System.out.println("queuedUpResponses size = "+queuedUpResponses.size());
        ByteArrayOutputStream bao = new ByteArrayOutputStream();
        DataOutputStream dos = new DataOutputStream(bao);
        int count = 0;
        while(!queuedUpResponses.isEmpty()){
          Serialization queuedUpSerializarion = queuedUpResponses.poll();
          dos.writeInt(queuedUpSerializarion.getSize());
          dos.writeShort(queuedUpSerializarion.getType());
          dos.write(queuedUpSerializarion.getBuffer());
          ++count;
        }
        // System.out.println("sending messages in "+(System.currentTimeMillis() - stime));
        JumboText jumboText = new JumboText();
        jumboText.setNumberOfItems(count);
        jumboText.setBytes(bao.toByteArray());
        responsePartial = new ResponsePartial(jumboText);
      } else {
        responsePartial = new ResponsePartial(queuedUpResponses.poll());
      }
      // create one jumbo package

      if(key.isValid()) {
        key.interestOps(SelectionKey.OP_WRITE | SelectionKey.OP_READ);
        key.selector().wakeup();
      }
    }
  }

  // will be executed by request/response thread.
  void writeResponse(){
    if(responsePartial != null){
      try {
        responsePartial.tryComplete(sc);
      } catch (IOException ioe){
        try {
          closeConnection();
        }
        catch (IOException e) {
          e.printStackTrace();
        }
      }
      if (responsePartial.complete()) {
        completeResponse();
      }
    }
  }

  private synchronized void completeResponse() {
    totalResponse.incrementAndGet();
    if(key.isValid()){
      key.interestOps(SelectionKey.OP_READ);
    }
    goForJumbo = true;
  }


  void readRequest(){
    if(pr != null){
      try {
        long stime = System.currentTimeMillis();
        pr.tryComplete(sc);

        if(pr.complete()){
          Task handleTask = pr.handle(this);
          broker.putOnCallback(handleTask);
        //  pr = new PartialRequest();
          if(System.currentTimeMillis() - stime > 1) {
            System.out.println(System.currentTimeMillis() - stime);
          }
        }

      } catch (IOException io){
        try {
          closeConnection();
        }
        catch (IOException e) {
          e.printStackTrace();
        }
      }
    }
  }

  private void closeConnection() throws IOException {
    queuedUpResponses.clear();
    Iterator<Long> keyIterator = sessions.keySet().iterator();
    while (keyIterator.hasNext()) {
      Long next = keyIterator.next();
      sessions.get(next).close();
    }
    this.broker.removeConnection(this.id);
    if(this.key.isValid())
      this.key.cancel();
    this.sc.close();
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

  public SessionContext registerSession(SessionInfo sessionInfo){
    SessionContext sessionContext = new SessionContext(sessionInfo);
    sessionContext.setConnectionContext(this);
    sessions.putIfAbsent(sessionInfo.getSessionId(), sessionContext);
    return sessionContext;
  }

  public PartialRequest getPr() {
    return pr;
  }

  public void setPr(PartialRequest pr) {
    this.pr = pr;
  }
}
