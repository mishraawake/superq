package org.apache.superq.db;

import java.io.IOException;
import java.util.Date;
import javax.jms.JMSException;

import org.apache.superq.QueueInfo;
import org.apache.superq.SMQDestination;
import org.apache.superq.SMQTextMessage;
import org.junit.Assert;
import org.junit.Test;

public class SerializationSupportTest {

  @Test
  public void serializeQInfo() throws IOException, JMSException {
    IndexEntry indexEntry = new IndexEntry();
    indexEntry.setMessageLocation(1);
    indexEntry.setMessageLength(10);
    IndexEntry indexEntry1  = new IndexEntry();
    indexEntry1.acceptByteBuffer(indexEntry.getBuffer());
    System.out.println(indexEntry1);
    Assert.assertEquals(indexEntry1, indexEntry);
  }


}
