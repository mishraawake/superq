package org.apache.superq.reqres;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import java.util.ArrayList;
import java.util.List;

import org.apache.superq.Serialization;
import org.apache.superq.Task;
import org.apache.superq.network.ConnectionContext;

public class MaxReaderPartialRequest extends PartialRequest {

  final int totalToRead = 1000000;
  private int remaining = totalToRead;
  ByteBuffer bigBuffer = ByteBuffer.allocate(totalToRead);
  byte[] currentArray;
  byte[] leftOverByte;
  public void tryComplete(SocketChannel ssc) throws IOException {
    int x = ssc.read(bigBuffer);
    remaining -= x;
  }

  private List<Serialization> parse(byte[] bigBuffer, ConnectionContext context) throws IOException {
    List<Serialization> serializationList = new ArrayList<>();
    if(leftOverByte != null){
      ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
      outputStream.write(leftOverByte);
      outputStream.write(bigBuffer);
      bigBuffer = outputStream.toByteArray();
    }
    ByteArrayInputStream inputStream = new ByteArrayInputStream(bigBuffer);
    DataInputStream dis = new DataInputStream(inputStream);
    int totalSize = bigBuffer.length;

    while(totalSize > 6){
      int length = dis.readInt();
      totalSize -= 4;
      messageType = dis.readShort();
      totalSize =- 2;
      if(totalSize > length){
        currentArray = new byte[length];
        dis.read(currentArray);

        try {
          Class classOfMessage = rf.getInitialPartialRequest(messageType);
          Serialization instantiate = (Serialization) classOfMessage.newInstance();
          instantiate.acceptByteBuffer(currentArray);
          serializationList.add(instantiate);
        } catch (Exception e){
          e.printStackTrace();;
        }

        totalSize -= length;
      } else {
        break;
      }
    }
    if(totalSize > 0) {
      leftOverByte = new byte[totalToRead - totalSize];
      dis.read(leftOverByte);
    } else {
      leftOverByte = null;
    }
    return serializationList;
  }

  public boolean complete(){
    return remaining == 0;
  }

  public Task handle(final ConnectionContext context){
    try {
      List<Serialization> serializationList = parse(bigBuffer.array(), context);;
      remaining = totalToRead;
      bigBuffer = ByteBuffer.allocate(totalToRead);
      return new Task(){
        @Override
        public void perform() throws Exception {
            Class classOfMessage = rf.getInitialPartialRequest(messageType);
            Serialization instantiate = (Serialization) classOfMessage.newInstance();
            instantiate.acceptByteBuffer(currentArray);
            for(Serialization indi : serializationList){
              handlerObject.get(indi.getClass().getName()).handle(indi, context);
            }
        }
      };
    }
    catch (IOException e) {
      e.printStackTrace();
    }
    return null;
  }
}
