package org.apache.superq.db;

import java.io.File;
import java.io.IOException;
import java.util.Date;
import javax.jms.JMSException;

import org.apache.superq.SMQDestination;
import org.apache.superq.SMQMessage;
import org.apache.superq.SMQTextMessage;
import org.junit.Before;

public class AbstractTest {
  public static String TEST_DATA_DIRECTORY = "test/data";
  protected String qname = "myq";
  protected FileDatabase<SMQMessage> fileDatabase;
  private SizeableFactory<SMQMessage> messageFactory = new MessageSizeableFactory();
  @Before
  public void setUp() throws IOException {

    if(new File(TEST_DATA_DIRECTORY).exists())
      for(File f: new File(TEST_DATA_DIRECTORY).listFiles()){
        if(f.exists())
          delete(f);
      }
    fileDatabase = new FileDatabase<SMQMessage>(TEST_DATA_DIRECTORY + "/"+qname, messageFactory);
    fileDatabase.setTyped();
  }

  private void delete(File file){
    if(file.isDirectory()){
      if(file.listFiles().length == 0){
        file.delete();
      } else {
        for(File f : file.listFiles()){
          delete(f);
        }
      }
    } else {
      file.delete();
    }
  }

  private static String getString(long index) throws IOException {
    StringBuilder sb = new StringBuilder();
    for (int i = 0; i < 50; i++) {
      sb.append("Hello First message ");
    }
    sb.append(index);
    return sb.toString();
  }

  protected void createMessage(int numberOfMessages) throws IOException, JMSException {
    for (int i = 0; i < numberOfMessages; i++) {
      fileDatabase.appendMessage(getMessage(i));
    }
  }

  private SMQTextMessage getMessage(long index) throws IOException, JMSException {
    SMQTextMessage textMessage = new SMQTextMessage();
    textMessage.setText(getString(index));
    textMessage.setJmsMessageLongId(1l);
    textMessage.setProducerId(2l);
    textMessage.setSessionId(3l);
    textMessage.setConnectionId(4l);
    textMessage.setConsumerId(5l);
    textMessage.setGroupId("Group");
    textMessage.setGroupSegId(1);
    textMessage.setJMSCorrelationID("correlation");
    textMessage.setJMSDeliveryMode(3);
    textMessage.setJMSDeliveryTime(new Date().getTime());
    textMessage.setJMSDestination(new SMQDestination() {
      @Override
      public int getDestinationId() {
        return 10;
      }
    });
    textMessage.setJMSExpiration(new Date().getTime());
    // textMessage.setJMSMessageID("jmsmessageId");
    textMessage.setJMSPriority(9);
    textMessage.setJMSRedelivered(false);
    textMessage.setJMSReplyTo(new SMQDestination() {
      @Override
      public int getDestinationId() {
        return 11;
      }

    });
    textMessage.setJMSTimestamp(new Date().getTime());
    textMessage.setTransactionId(100);
    return textMessage;
  }
}
