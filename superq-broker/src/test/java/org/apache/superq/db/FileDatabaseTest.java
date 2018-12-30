package org.apache.superq.db;

import java.io.File;
import java.io.IOException;
import java.util.Date;
import java.util.Random;

import javax.jms.JMSException;

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
      //if(f.exists())
        //f.delete();
    }
    fileDatabase = new FileDatabase<SMQTextMessage>(TEST_DATA_DIRECTORY + "/myq", messageFactory);
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
    SMQTextMessage message = new SMQTextMessage();
    message.setText(getString(index));
    return message;
  }



  private static String getString(long index) throws IOException {
    StringBuilder sb = new StringBuilder();
    for (int i = 0; i < 10; i++) {
      sb.append("Hello First message ");
    }
    sb.append(index);
    return sb.toString();
  }


}
