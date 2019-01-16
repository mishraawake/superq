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
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.superq.Broker;
import org.apache.superq.ConnectionInfo;
import org.apache.superq.CorrelatedPacket;
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
  AtomicInteger totalSendResponse =  new AtomicInteger(0);
  AtomicInteger totalWrote =  new AtomicInteger(0);
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
    totalSendResponse.incrementAndGet();
    if(serialization instanceof CorrelatedPacket){
      CorrelatedPacket correlatedPacket = (CorrelatedPacket) serialization;
    }
    if(goForJumbo){
      writeAccumulatedResponse();
      // create one jumbo package
    }
  }

  private synchronized void writeAccumulatedResponse() throws IOException {
    if(!goForJumbo){
      return;
    }
    if(queuedUpResponses.isEmpty()){
      return;
    }
    goForJumbo = false;
    if(queuedUpResponses.size() > 1){
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
      totalWrote.addAndGet(count);
      responsePartial = new ResponsePartial(jumboText);
    } else {
      totalWrote.incrementAndGet();
      responsePartial = new ResponsePartial(queuedUpResponses.poll());
    }
    if(key.isValid()) {
      key.interestOps(SelectionKey.OP_WRITE | SelectionKey.OP_READ);
      key.selector().wakeup();
    }
  }

  // will be executed by request/response thread.
  void writeResponse() throws IOException {
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
        ioe.printStackTrace();
      }
      if (responsePartial.complete()) {
        completeResponse();
      }
    }
  }

  private synchronized void completeResponse() throws IOException {
    totalResponse.incrementAndGet();
    if(key.isValid()){
      key.interestOps(SelectionKey.OP_READ);
    }
    goForJumbo = true;
    writeAccumulatedResponse();
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
    System.out.println("killing connection "+sc);
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
