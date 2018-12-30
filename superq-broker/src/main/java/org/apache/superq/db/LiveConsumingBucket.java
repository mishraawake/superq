package org.apache.superq.db;

import java.util.Date;
import java.util.Iterator;
import java.util.List;

/**
 * There will be one per queue. It will have all the unack messages, rollback messages. In live scenario whenever
 * there will be ack of some message delete, it will delete that message from this database and fetch few more
 * depending upon the consumers outstanding
 */
public class LiveConsumingBucket {
  List<IndexEntry> unackMessages;
  List<IndexEntry> tobeRedelivered;
  IndexEntry offset;

  public void ack(long messageId){
    Iterator<IndexEntry> iterator = unackMessages.iterator();
    while(iterator.hasNext()){
      if(iterator.next().getMessageLocation() == messageId){
        iterator.remove();
      }
    }
  }


  class UnackMessage {
    IndexEntry indexEntry;
    Date lastAccess;
  }
}
