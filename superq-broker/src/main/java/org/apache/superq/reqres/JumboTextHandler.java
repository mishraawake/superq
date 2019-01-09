package org.apache.superq.reqres;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.IOException;

import org.apache.superq.ConstArrayWiredObjectFactory;
import org.apache.superq.JumboText;
import org.apache.superq.SMQMessage;
import org.apache.superq.SMQTextMessage;
import org.apache.superq.Serialization;
import org.apache.superq.WiredObjectFactory;
import org.apache.superq.network.ConnectionContext;

public class JumboTextHandler implements RequestHandler<JumboText> {
  WiredObjectFactory rf = new ConstArrayWiredObjectFactory();
  @Override
  public void handle(JumboText serialization, ConnectionContext connectionContext) {

    try {
      ByteArrayInputStream inputStream = new ByteArrayInputStream(serialization.getBytes());
      DataInputStream dataInputStream = new DataInputStream(inputStream);
      int count = 0;
      Serialization first = null;
      while(count < serialization.getNumberOfItems()){
        int length = dataInputStream.readInt();
        short type = dataInputStream.readShort();
        byte[] bytes = new byte[length];
        dataInputStream.read(bytes);
        Class classOfMessage = rf.getInitialPartialRequest(type);
        Serialization instantiate = (Serialization)classOfMessage.newInstance();
        instantiate.acceptByteBuffer(bytes);
        if(first == null)
          first = instantiate;

        PartialRequest.handlerObject.get(classOfMessage.getName()).handle(instantiate, connectionContext);
        ++count;
      }
      if (count > 1 && !(first instanceof SMQMessage))
        System.out.println("Jumbo handler "+count+" "+first.getClass());
    }
    catch (Exception e) {
      e.printStackTrace();
    }
  }
}
