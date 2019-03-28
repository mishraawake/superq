package org.apache.superq.reqres;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.superq.Broker;
import org.apache.superq.ConstArrayWiredObjectFactory;
import org.apache.superq.CorrelatedPacket;
import org.apache.superq.WiredObjectFactory;
import org.apache.superq.Serialization;
import org.apache.superq.Task;
import org.apache.superq.network.BrokerServer;
import org.apache.superq.network.ConnectionContext;

public class PartialRequest implements Partial {

  public PartialRequest(){
  }

  protected BrokerServer ser;
  protected Broker broker;
  protected SocketChannel ssc;
  protected boolean startedReading = false;
  private final int INT_PLUS_MESSAGE_TYE = 4 + 2;
  protected int remaining = INT_PLUS_MESSAGE_TYE + 1;
  ByteBuffer size = ByteBuffer.allocate(INT_PLUS_MESSAGE_TYE);
  WiredObjectFactory rf = new ConstArrayWiredObjectFactory();
  private PartialRequest partialRequest = null;
  private boolean emptyByte = true;
  short messageType;
  static Map<String, RequestHandler> handlerObject = new ConcurrentHashMap<>();
  long stime = System.currentTimeMillis();

  static {
    for(Class classToMap : WiredObjectFactory.classTypes ){
      String handlerName = "org.apache.superq.reqres." + classToMap.getSimpleName() + "Handler";
      RequestHandler classOfMessage = null;
      try {
        classOfMessage = (RequestHandler)Class.forName(handlerName).newInstance();
      }
      catch (ClassNotFoundException | InstantiationException | IllegalAccessException e) {
        throw  new RuntimeException("Handlers for "+handlerName+" is not defined");
      }
      handlerObject.put(classToMap.getName(), classOfMessage);
    }
  }

  protected ByteBuffer backingArray;

  public void tryComplete(SocketChannel ssc) throws IOException {

    if(!startedReading){
      stime = System.currentTimeMillis();
      readOrThrowException(ssc, size);
     // System.out.println("Accept time1 "+(System.currentTimeMillis() - stime));
      startedReading = true;
      return;
    }
    if(size.position() < INT_PLUS_MESSAGE_TYE){
      readOrThrowException(ssc, size);
     // System.out.println("Accept time2 "+(System.currentTimeMillis() - stime));
      return;
    } else if(emptyByte) {
     // System.out.println("Accept time3 "+(System.currentTimeMillis() - stime));
      size.flip();
      remaining = size.getInt();
      messageType = size.getShort();
      backingArray = ByteBuffer.allocate(remaining);
      remaining -= readOrThrowException(ssc, backingArray);
      emptyByte = false;
      //if((System.currentTimeMillis() - stime) > 10)
       // System.out.println("Accept time3 "+(System.currentTimeMillis() - stime));
      return;
    }
    if(remaining > 0){
      remaining -= readOrThrowException(ssc, backingArray);
     // System.out.println("Accept time4 "+(System.currentTimeMillis() - stime));
    } else {
      backingArray.flip();
    }
  }

  protected int readOrThrowException(SocketChannel ssc, ByteBuffer bb) throws IOException {
    long stime = System.currentTimeMillis();
    int readBytes = ssc.read(bb);
   // System.out.println("Raw read time " + (System.currentTimeMillis() - stime));
    if(readBytes < 0){
      throw new IOException("Channel "+ssc);
    }
    return readBytes;
  }

  public boolean complete(){
    return remaining == 0;
  }

  @Override
  public Task handle(final ConnectionContext context) throws IOException {
    try {
      startedReading = false;
      remaining = INT_PLUS_MESSAGE_TYE + 1;
      size = ByteBuffer.allocate(INT_PLUS_MESSAGE_TYE);
      emptyByte = true;
      byte[] array = backingArray.array();
     // System.out.println("messageType = " + messageType);
      Class classOfMessage = rf.getInitialPartialRequest(messageType);
      final Serialization instantiate = (Serialization) classOfMessage.newInstance();

      synchronized (PartialRequest.this) {
        instantiate.acceptByteBuffer(array);
      }
     // System.out.println("Accept time "+(System.currentTimeMillis() - stime));
      if(instantiate instanceof CorrelatedPacket) {
        CorrelatedPacket packet = (CorrelatedPacket) instantiate;
        if (System.currentTimeMillis() - packet.getDebugTime() > 1) {
          //System.out.println("Debug time in handle = " + (System.currentTimeMillis() - packet.getDebugTime()));
        }
      }
      Task task = new Task() {
        @Override
        public void perform() throws Exception {
          handlerObject.get(classOfMessage.getName()).handle(instantiate, context);
        }
      };
      return task;
    } catch (Exception e){
      throw new IOException(e);
    }
  }

  public BrokerServer getSer() {
    return ser;
  }

  public void setSer(BrokerServer ser) {
    this.ser = ser;
  }

}
