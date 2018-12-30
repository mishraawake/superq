package org.apache.superq.storage;

import java.io.IOException;
import java.util.List;

import org.apache.superq.SMQMessage;
import org.apache.superq.db.FileDatabase;
import org.apache.superq.db.FileDatabaseFactoryImpl;

public class MessageStoreImpl implements MessageStore {

  FileDatabase<SMQMessage> fd;
  private int pageSize = 10;
  List<SMQMessage> messageFetched = null;

  public MessageStoreImpl(String qname) throws IOException {
     fd = FileDatabaseFactoryImpl.getInstance().
            getOrInitializeMainDatabase(qname);
  }

  @Override
  public SMQMessage getNextMessage() throws IOException {
    if(messageFetched == null || messageFetched.size() == 0) {
      messageFetched = fd.getOldMessage(pageSize);
    }
    SMQMessage message = messageFetched.get(0);
    messageFetched.remove(0);
    return message;
  }

  @Override
  public boolean hasMoreMessage() {
    return false;
  }

  @Override
  public void addMessage(SMQMessage smqMessage) throws IOException {
    fd.appendMessage(smqMessage);
  }

  @Override
  public boolean doesSpaceLeft() {
    return false;
  }
}
