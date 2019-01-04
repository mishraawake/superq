package org.apache.superq.db;

import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicLong;

import io.reactivex.Single;
import org.apache.superq.SMQMessage;
import org.apache.superq.Serialization;

/**
 * Requirement of a queueing system:
 * - Append of newly coming committed/uncommitted message
 * - Consumption of message, a moving marker from the behind.
 *    Taking care of slow consumers -> We will have small island scatter everywhere. Strategy is to get those
 *    island together.
 * - A secondary storage for all the message which is to be redelivered with frequency.
 * - A dead-later-queue
 *
 */

public class FileDatabase<T extends Serialization> {

  AtomicLong continuously_increasing_id = new AtomicLong(-1);
  OneRow<IndexEntry> indexRow;
  SizeableFactory<IndexEntry> indexEntryFactory = new EntrySizeableFactory<IndexEntry>();
  OneRow<T> messageRow;

  public FileDatabase(String dbLocation, SizeableFactory<T> messageFactory) throws IOException {
    indexRow = new OneRow<IndexEntry>(dbLocation, "i", indexEntryFactory);
    indexRow.setName("index");
    //indexRow.mainInMemory();
    continuously_increasing_id.set(((indexRow.medadata.getProcessedSize() + indexRow.medadata.getSize())/ IndexEntry.SIZE)  -1);
    messageRow = new OneRow<T>(dbLocation,  "m", messageFactory);
    messageRow.setName("message");
  }


  public boolean appendMessage(T message) throws IOException {
    long messageMCIndex = messageRow.append(message);
    IndexEntry entry = new IndexEntry();
    entry.setMessageLocation(messageMCIndex);
    if(messageRow.isTyped())
      entry.setMessageLength(message.getSize() + Short.BYTES);
    else
      entry.setMessageLength(message.getSize());
    long nextLong = continuously_increasing_id.incrementAndGet();
    indexRow.append(entry, nextLong*IndexEntry.SIZE);
    return true;
  }

  public synchronized Single appendViaReactive(T message) throws IOException {
    return Single.create( subscriber -> {
      subscriber.onSuccess(appendMessage(message));
    });
  }

  public T getMessage(long messageId) throws IOException {
    long messageLocationInIndexFile = getMessageIndexLocation(messageId);

    IndexEntry indexEntry = indexRow.getEntry(messageLocationInIndexFile, IndexEntry.SIZE);
    if(indexEntry.getMessageLocation() < 0){
      return null;
    }
    return getMessageFromMessageDB(indexEntry, messageId);
  }

  public synchronized List<T> getAllMessage() throws IOException {
    long i = 0;
    IndexEntry indexEntry = null;
    List<T> list = new ArrayList<>();
    do{
        indexEntry = indexRow.getEntry(i*IndexEntry.SIZE, IndexEntry.SIZE);
        ++i;
        if(indexEntry == null || indexEntry.getMessageLength() == 0){
          break;
        }
        list.add(getMessageFromMessageDB(indexEntry, i));

    } while(indexEntry != null);
    return list;
  }



  private long getMessageIndexLocation(long messageId){
    return messageId*IndexEntry.SIZE;
  }

  private T getMessageFromMessageDB(IndexEntry indexEntry, long messageId) throws IOException {
    T result = messageRow.getEntry(indexEntry.getMessageLocation() -
                                messageRow.getProcessedSize(), indexEntry.getMessageLength());
    if(result instanceof SMQMessage){
      ((SMQMessage)result).setJmsMessageLongId(messageId);
    }
    return result;
  }

  public void deleteMessage(long messageId) throws IOException{
    long messageLocationInIndexFile = getMessageIndexLocation(messageId);
    IndexEntry ie = new IndexEntry();
    ie.setMessageLocation(-1);
    indexRow.update(messageLocationInIndexFile, ie.getBuffer());
    updateLeftoverOffset();
  }

  private void updateLeftoverOffset() throws IOException {
    long leftoverOffset = indexRow.medadata.leftoverOffset(),  leftOverCounter = leftoverOffset +  1;
    while(indexRow.getEntry(leftOverCounter, IndexEntry.SIZE) == null){
      ++leftOverCounter;
    }
    if(leftOverCounter - 1 != leftoverOffset){
      indexRow.medadata.leftoverOffset(leftOverCounter - 1);
    }
  }


  private static void startCreatingThread(FileDatabase fd) throws InterruptedException {
    Runnable rn = new Runnable() {
      @Override
      public void run() {
        int count = 0;
        long stime = System.currentTimeMillis();
        while(count++ <100000){

           // create(fd, count);
            if(count %100000 == 0){
              System.out.println(System.currentTimeMillis() - stime + "   "+ Thread.currentThread().getName());
              stime = System.currentTimeMillis();
            }
        }
      }
    };
    List<Thread> list = new ArrayList<>();
    for (int i = 0; i < 1; i++) {
      Thread createT1 = new Thread(rn, "t"+i);
      createT1.start();
      list.add(createT1);
    }
    for(Thread t : list){
      t.join();
    }

  }

  private static void appendViaReactive(FileDatabase fd) throws IOException {
    CountDownLatch cdl = new CountDownLatch(10000_00);
  }



  private static void findMessage(FileDatabase fd, long position) throws IOException {
    fd.getMessage(position);
  }


  public void setTyped() {
    messageRow.setTyped();
  }

  public static void main(String[] args) {
    for (int i = 0; i < 10; i++) {
     // System.out.println(continuously_increasing_id.incrementAndGet());
    }
  }

  public List<T> getOldMessage(int additional) throws IOException {
    long startIndex = indexRow.medadata.messageOffset();
    List<T> list = new ArrayList<>();
    long i = startIndex;
    for(; i < startIndex + additional ; ++i){
      IndexEntry indexEntry = indexRow.getEntry(i*IndexEntry.SIZE, IndexEntry.SIZE);
      if(indexEntry ==  null){
        break;
      }
      if(indexEntry.getMessageLength() == 0){
        System.out.println("foota hua index entry ="+startIndex);
        i++;
        break;
      }
      T message = getMessageFromMessageDB(indexEntry, i);
      list.add(message);
    }
    if(startIndex != i)
       indexRow.medadata.messageOffset(i);
    return list;
  }

  public List<T> browseOldMessage(int additional, Long messageId) throws IOException {
    long startIndex = indexRow.medadata.leftoverOffset();
    if(messageId != null){
      startIndex = messageId;
    }

    List<T> list = new LinkedList<>();

    for (long messageIndex = startIndex ; list.size() <  additional; messageIndex++) {
      IndexEntry indexEntry = indexRow.getEntry(messageIndex*IndexEntry.SIZE, IndexEntry.SIZE);
      if(indexEntry ==  null){
        break;
      }
      if(indexEntry.getMessageLength() == 0){
        System.out.println("foota hua index entry ="+startIndex);
        messageIndex++;
        break;
      }
      T message = getMessageFromMessageDB(indexEntry, messageIndex);
      list.add(message);
    }
    return list;
  }

  public List<T> getUnackMessages() throws IOException {

    long startIndex = indexRow.medadata.leftoverOffset();
    long endIndex = indexRow.medadata.messageOffset() - 1;
    List<T> list = new ArrayList<>();

    for (long messageIndex = startIndex ; messageIndex < endIndex; messageIndex++) {
      IndexEntry indexEntry = indexRow.getEntry(messageIndex*IndexEntry.SIZE, IndexEntry.SIZE);
      if(indexEntry ==  null){
        break;
      }
      if(indexEntry.getMessageLength() == 0){
        System.out.println("foota hua index entry ="+startIndex);
        messageIndex++;
        break;
      }
      if(indexEntry.getMessageLocation() == 0){
        continue;
      }
      T message = getMessageFromMessageDB(indexEntry, messageIndex);
      list.add(message);
    }
    return list;
  }


}
