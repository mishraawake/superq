package org.apache.superq.db;

import java.io.File;
import java.io.IOException;
import java.util.List;

import javax.jms.JMSException;

import org.apache.superq.QueueInfo;
import org.junit.Before;
import org.junit.Test;

public class InfoFileDBTest extends AbstractTest {

  FileDatabase<QueueInfo> fileDatabase;
  SizeableFactory<QueueInfo> messageFactory = new InfoSizeableFactory();

  @Before
  public void setUp() throws IOException {

    if(new File(TEST_DATA_DIRECTORY).exists())
      for(File f: new File(TEST_DATA_DIRECTORY).listFiles()){
        //if(f.exists())
         // f.delete();
      }
    fileDatabase = new FileDatabase<QueueInfo>(TEST_DATA_DIRECTORY + "/myq", messageFactory);
  }

  @Test
  public void testMessageAdd() throws IOException, JMSException {
    QueueInfo info = new QueueInfo();
    info.setQueueName("nameofqueue");
    info.setPacketId(10);
    info.setId(10);
    fileDatabase.setTyped();
   // fileDatabase.appendMessage(info);
    List<QueueInfo> qinfo = fileDatabase.getAllMessage();
    System.out.println(qinfo);
  }

}
