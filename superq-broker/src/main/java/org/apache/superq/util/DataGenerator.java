package org.apache.superq.util;

import java.io.IOException;
import java.util.Date;
import java.util.Random;

import javax.jms.JMSException;

import org.apache.superq.SMQMessage;
import org.apache.superq.SMQTextMessage;
import org.apache.superq.Serialization;
import org.apache.superq.db.FileDatabase;
import org.apache.superq.db.IndexEntry;
import org.apache.superq.db.OneRow;
import org.apache.superq.db.Sizeable;
import org.apache.superq.db.SizeableFactory;

public class DataGenerator {

  private FileDatabase<SMQMessage> getFileDB(String dir) throws IOException {
   return new FileDatabase(dir + "/myq", new SizeableFactory() {
      @Override
      public Serialization getSizeable() {
        return new SMQTextMessage();
      }

      @Override
      public Serialization getSizeableByType(short type) {
        return null;
      }
    });
  }
  public void generateHugeData(String dir, long record) throws IOException  {
    FileDatabase fileDatabase = getFileDB(dir);
    long overFlowSize = (OneRow.APPEND_MC_SIZE / IndexEntry.SIZE )*20;
    if(record != 0){
      overFlowSize = record;
    }

    System.out.println("generating "+overFlowSize+" record");
    int length = getString(1).length() - 1;
    long stime = System.currentTimeMillis();

    for (long i = 0; i < overFlowSize; i++) {
      fileDatabase.appendMessage(getMessage(i));
      if(i % 100000 == 0){
        System.out.println( "generated "+ ( System.currentTimeMillis() - stime) + " "+i);
      }
    }
  }

  public void hugeDataReader(String dir, int times) throws IOException, JMSException {
    FileDatabase<SMQMessage> fileDatabase = getFileDB(dir);
    if(times == 0){
      times = 1000_000;
    }
    long overFlowSize = (OneRow.APPEND_MC_SIZE / IndexEntry.SIZE )*20;
    //System.out.println(overFlowSize);
    int length = getString(1).length() - 1;
    long stime = System.currentTimeMillis();

    SMQTextMessage message = null;
    stime = System.currentTimeMillis();
    Random rand = new Random();
    for (long i = 1; i < times ; i++) {

      int index = rand.nextInt((int)overFlowSize);
      if(index == 0){
        index = 1;
      }

      message = (SMQTextMessage) fileDatabase.getMessage(index);
      //message.setContent();
      if(!message.getText().substring(length).equals(((SMQTextMessage) getMessage(index-1)).getText().substring(length))){
       // System.out.println("MyMessage Content did not match" + " "+message.getContent().substring(length) +"==="+getMessage(index-1).getContent().substring(length));
      }

      if(i%100_000 == 0)
        System.out.println("read "+ i +" message in "+(System.currentTimeMillis() - stime));
    }
    System.out.println(" "+(System.currentTimeMillis() - stime));
  }


  public void seqDataReader(String dir, int times) throws IOException, JMSException {
    FileDatabase<SMQMessage> fileDatabase = getFileDB(dir);
    if(times == 0){
      times = 1000_000;
    }
    long overFlowSize = (OneRow.APPEND_MC_SIZE / IndexEntry.SIZE )*20;
    //System.out.println(overFlowSize);
    int length = getString(1).length() - 1;
    long stime = System.currentTimeMillis();

    SMQTextMessage message = null;
    stime = System.currentTimeMillis();
    Random rand = new Random();
    for (long i = 1; i < times ; i++) {

      int index = (int)i;
      if(index == 0){
        index = 1;
      }

      message = (SMQTextMessage) fileDatabase.getMessage(index);
      //message.setContent();
      if(!message.getText().substring(length).equals(((SMQTextMessage) getMessage(index-1)).getText().substring(length))){
        //System.out.println("MyMessage Content did not match" + " "+message.getContent().substring(length) +"==="+getMessage(index-1).getContent().substring(length));
      }

      if(i%100_000 == 0)
        System.out.println("read "+ i +" message in "+(System.currentTimeMillis() - stime));
    }
    System.out.println(" "+(System.currentTimeMillis() - stime));
  }

  private SMQMessage getMessage(long index) throws IOException{
    SMQMessage message = new SMQTextMessage();
    try {
      ((SMQTextMessage) message).setText(getString(index));
    }
    catch (JMSException e) {
      e.printStackTrace();
    }
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

  public static void main(String[] args) throws IOException, JMSException {

    String action = args.length > 0 ? args[0]: "generate";
    long record = args.length > 1 ? Long.valueOf(args[1]): 0;

    DataGenerator dataGenerator = new DataGenerator();
    if(action.equals("generate")){
      dataGenerator.generateHugeData("data", record);
    } else if(action.equals("seq")) {
      dataGenerator.seqDataReader("data", (int)record);
    } else {
      dataGenerator.hugeDataReader("data", (int)record);
    }
    //

   //
  }



}
