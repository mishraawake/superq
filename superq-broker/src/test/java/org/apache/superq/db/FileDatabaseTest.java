package org.apache.superq.db;

import java.io.File;
import java.io.IOException;
import java.util.Date;
import java.util.List;
import java.util.Random;

import javax.jms.JMSException;

import org.apache.superq.SMQDestination;
import org.apache.superq.SMQTextMessage;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class FileDatabaseTest extends AbstractTest{

  FileDatabase<SMQTextMessage> fileDatabase;
  SizeableFactory<SMQTextMessage> messageFactory = new MessageSizeableFactory();
  @Before
  public void setUp() throws IOException {

    if(new File(TEST_DATA_DIRECTORY).exists())
    for(File f: new File(TEST_DATA_DIRECTORY).listFiles()){
      if(f.exists())
        f.delete();
    }
    fileDatabase = new FileDatabase<SMQTextMessage>(TEST_DATA_DIRECTORY + "/myq", messageFactory);
    fileDatabase.setTyped();
  }

  @Test
  public void testMessageREtrieval() throws IOException, JMSException {

    int counter = 0;
    long stime = System.currentTimeMillis();
    while(++counter < 2) {
      for (int messageIndex = 0; messageIndex < 1000000; messageIndex++) {
        fileDatabase.appendMessage(getMessage(messageIndex));
      }
      System.out.println("Total time spend" + (stime - System.currentTimeMillis()));

      for (int batch = 0; batch < 10; batch++) {
        List<SMQTextMessage> messagesList = fileDatabase.getOldMessage(100000);
        Assert.assertEquals(messagesList.size(), 100000);
      }

      Assert.assertEquals(fileDatabase.getOldMessage(100).size(), 0);
    }

    System.out.println("Total time spend" + (stime - System.currentTimeMillis()));
  }

  @Test
  public void testMessageAdd() throws IOException, JMSException {
    long overFlowSize = (OneRow.APPEND_MC_SIZE / IndexEntry.SIZE )*20;
    System.out.println(overFlowSize);
    int length = getString(1).length() - 1;
    long stime = System.currentTimeMillis();

    for (long i = 0; i < overFlowSize; i++) {
      fileDatabase.appendMessage(getMessage(i));
      if(i % 100000 == 0){
        System.out.println( System.currentTimeMillis() - stime);
      }
    }
    System.out.println( "finished = "+( System.currentTimeMillis() - stime));


    SMQTextMessage message = null;
    stime = System.currentTimeMillis();
    for (long i = 1; i < overFlowSize ; i++) {
      message = fileDatabase.getMessage(i);
      //message.setContent();
      Assert.assertEquals(message.getText().substring(length), getMessage(i-1).getText().substring(length));
     if(i%100000 == 0)
      System.out.println(i +" "+(System.currentTimeMillis() - stime));
    }
    System.out.println(" "+(System.currentTimeMillis() - stime));
  }


  @Test
  public void randomMesssageFind() throws IOException, JMSException {
    long overFlowSize = (OneRow.APPEND_MC_SIZE / IndexEntry.SIZE )*20;
    //System.out.println(overFlowSize);
    int length = getString(1).length() - 1;
    long stime = System.currentTimeMillis();


    SMQTextMessage message = null;
    stime = System.currentTimeMillis();
    Random rand = new Random();
    for (long i = 1; i < 100_0000 ; i++) {

      int index = rand.nextInt((int)overFlowSize);
      if(index == 0){
        index = 1;
      }

      message = fileDatabase.getMessage(index);
      //message.setContent();
      Assert.assertEquals(message.getText().substring(length), getMessage(index-1).getText().substring(length));
      if(i%100_000 == 0)
        System.out.println(i +" "+(System.currentTimeMillis() - stime));
    }
    System.out.println(" "+(System.currentTimeMillis() - stime));
  }

  @Test
  public void testMessageDelete() throws IOException, JMSException {
    for (int i = 0; i < 100; i++) {
      fileDatabase.appendMessage(getMessage(i));
    }

    SMQTextMessage message = fileDatabase.getMessage(10);
    System.out.println(message);
    Assert.assertNotNull(message);
    fileDatabase.deleteMessage(10);

    message = fileDatabase.getMessage(10);
    System.out.println(message);
    Assert.assertNull(message);

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



  private static String getString(long index) throws IOException {
    StringBuilder sb = new StringBuilder();
    for (int i = 0; i < 50; i++) {
      sb.append("Hello First message ");
    }
    sb.append(index);
    return sb.toString();
  }


}
