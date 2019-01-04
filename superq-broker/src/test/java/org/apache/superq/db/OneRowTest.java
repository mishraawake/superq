package org.apache.superq.db;

import java.io.File;
import java.io.IOException;

import org.apache.superq.SMQMessage;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class OneRowTest extends AbstractTest {

  OneRow<IndexEntry> indexOneRow;
  OneRow<SMQMessage> messageOneRow;
  SizeableFactory<IndexEntry> indexEntryFactory = new EntrySizeableFactory<IndexEntry>();
  SizeableFactory<SMQMessage> messageFactory ;
  @Before
  public void setUp() throws IOException {

    for(File f: new File(TEST_DATA_DIRECTORY).listFiles()){
      f.delete();
    }

    indexOneRow = new OneRow<IndexEntry>(TEST_DATA_DIRECTORY, "indexrow", indexEntryFactory);
    messageOneRow = new OneRow<SMQMessage>(TEST_DATA_DIRECTORY, "messagerow", messageFactory);
  }

  @Test
  public void testBasicIndexAppend() throws IOException {
    int entries = 100;
    insertEntries(entries);
    Assert.assertEquals(indexOneRow.memoryCell.getLastValidLocation(), entries*IndexEntry.SIZE);
    Assert.assertEquals(indexOneRow.medadata.getSize(), entries*IndexEntry.SIZE);
  }



  @Test
  public void testWithPrcessedBasicIndexAppend() throws IOException {
    int entries = 100;
    int processedEntries = 1000;
    indexOneRow.medadata.setProcessedSize(processedEntries*IndexEntry.SIZE);
    insertEntries(entries);
    Assert.assertEquals(indexOneRow.memoryCell.getLastValidLocation(), entries*IndexEntry.SIZE);
    Assert.assertEquals(indexOneRow.medadata.getSize(), entries*IndexEntry.SIZE);
  }

  @Test
  public void testBasicInvalidIndexAppend() throws IOException {
    int entries = 100;

    insertEntries(entries);
    IndexEntry indexEntry = getIndexEntry();
    boolean exception = false;
    try {
      indexOneRow.append(indexEntry, entries * IndexEntry.SIZE - 10);
    } catch (RuntimeException e){
      exception = true;
    }
    Assert.assertTrue(exception);
    indexOneRow.append(indexEntry, entries * IndexEntry.SIZE );
    entries++;
    Assert.assertEquals(indexOneRow.memoryCell.getLastValidLocation(), entries*IndexEntry.SIZE );
    Assert.assertEquals(indexOneRow.medadata.getSize(), entries*IndexEntry.SIZE);
  }

  @Test
  public void testProcessedBasicInvalidIndexAppend() throws IOException {
    int entries = 100;
    int processedEntries = 1000;
    indexOneRow.medadata.setProcessedSize(processedEntries*IndexEntry.SIZE);
    insertEntries(entries);
    IndexEntry indexEntry = getIndexEntry();
    boolean exception = false;
    try {
      indexOneRow.append(indexEntry, processedEntries*IndexEntry.SIZE + entries * IndexEntry.SIZE - 10);
    } catch (RuntimeException e){
      exception = true;
    }
    Assert.assertTrue(exception);
    indexOneRow.append(indexEntry, processedEntries*IndexEntry.SIZE + entries * IndexEntry.SIZE );
    entries++;
    Assert.assertEquals(indexOneRow.memoryCell.getLastValidLocation(), entries*IndexEntry.SIZE );
    Assert.assertEquals(indexOneRow.medadata.getSize(), entries*IndexEntry.SIZE);
  }

  private IndexEntry getIndexEntry(long messageLocation){
    IndexEntry indexEntry = new IndexEntry();
    indexEntry.setMessageLocation(messageLocation);
    indexEntry.setMessageLength(0);
    return indexEntry;
  }

  private IndexEntry getIndexEntry(){
    return getIndexEntry(0);
  }

  @Test
  public void testScatteredIndexAppend() throws IOException {
    int entries = 100;
    int gap = 10000;
    insertEntries(entries);
    IndexEntry indexEntry = getIndexEntry();
    // System.out.println(i);
    indexOneRow.append(indexEntry, entries*IndexEntry.SIZE + gap);
    Assert.assertEquals(indexOneRow.memoryCell.getLastValidLocation(), (entries + 1)*IndexEntry.SIZE + gap);
    Assert.assertEquals(indexOneRow.medadata.getSize(), (entries + 1)*IndexEntry.SIZE + gap);
  }

  @Test
  public void testProcessedScatteredIndexAppend() throws IOException {
    int entries = 100;
    int gap = 10000;
    indexOneRow.medadata.setProcessedSize(10000);
    insertEntries(entries);
    IndexEntry indexEntry = getIndexEntry();
    // System.out.println(i);
    indexOneRow.append(indexEntry, 10000 + entries*IndexEntry.SIZE + gap);
    Assert.assertEquals(indexOneRow.memoryCell.getLastValidLocation(), (entries + 1)*IndexEntry.SIZE + gap);
    Assert.assertEquals(indexOneRow.medadata.getSize(), (entries + 1)*IndexEntry.SIZE + gap);
  }

  private void insertEntries(int entry) throws IOException {
    IndexEntry indexEntry;
    long startingLocation = indexOneRow.medadata.getProcessedSize() + indexOneRow.medadata.getSize();
    int startingIndex = (int)(startingLocation / IndexEntry.SIZE);

    startingIndex = startingLocation%IndexEntry.SIZE == 0 ? startingIndex : startingIndex + 1;
    for (int i = startingIndex ; i < startingIndex + entry; i++) {
      indexEntry = getIndexEntry(i - startingIndex);
      // System.out.println(i);
      indexOneRow.append(indexEntry, i*IndexEntry.SIZE);
    }
  }


  @Test
  public void testEdgeGet() throws IOException {
    int entries = (int)(OneRow.APPEND_MC_SIZE / IndexEntry.SIZE) + 100;
    insertEntries(entries);
//    Assert.assertEquals(indexOneRow.memoryCell.getLastValidLocation(), entries*IndexEntry.SIZE);
    Assert.assertEquals(indexOneRow.medadata.getSize(), entries*IndexEntry.SIZE);

    IndexEntry entry = indexOneRow.getEntry(0, IndexEntry.SIZE);

    Assert.assertEquals(entry.getMessageLocation(), 0);

    entry = indexOneRow.getEntry(IndexEntry.SIZE*111,IndexEntry.SIZE);
    Assert.assertEquals(entry.getMessageLocation(), 111);

    entries -= 100;
    for (int i = 0; i < 100; i++) {
      entry = indexOneRow.getEntry(IndexEntry.SIZE*(entries + i ), IndexEntry.SIZE);
      Assert.assertEquals(entry.getMessageLocation(), entries + i );
    }
  }


  @Test
  public void testProcessedEdgeGet() throws IOException {
    int entries = (int)(OneRow.APPEND_MC_SIZE / IndexEntry.SIZE) + 100;
    int processedEntries = 1000;
    indexOneRow.medadata.setProcessedSize(processedEntries*IndexEntry.SIZE);
    insertEntries(entries);

//    Assert.assertEquals(indexOneRow.memoryCell.getLastValidLocation(), entries*IndexEntry.SIZE);
    Assert.assertEquals(indexOneRow.medadata.getSize(), entries*IndexEntry.SIZE);

    IndexEntry entry = indexOneRow.getEntry(processedEntries * IndexEntry.SIZE, IndexEntry.SIZE);

    Assert.assertEquals(entry.getMessageLocation(), 0);

    entry = indexOneRow.getEntry(processedEntries * IndexEntry.SIZE + IndexEntry.SIZE*113,IndexEntry.SIZE);
    Assert.assertEquals(entry.getMessageLocation(), 113);

    entries -= 100;
    for (int i = 0; i < 100; i++) {
      entry = indexOneRow.getEntry(processedEntries * IndexEntry.SIZE + IndexEntry.SIZE*(entries + i ), IndexEntry.SIZE);
      Assert.assertEquals(entry.getMessageLocation(), entries + i );
    }
  }

  @Test
  public void testDeleteorupdate() throws IOException {
    int entries = (int)(OneRow.APPEND_MC_SIZE / IndexEntry.SIZE ) + 100;
    insertEntries(entries);
    IndexEntry entry = new IndexEntry();
    entry.setMessageLength(0);
    entry.setMessageLocation(100000123123l);
  //  entry.getBuffer().flip();
    indexOneRow.update(IndexEntry.SIZE*10,entry.getBuffer());
    entry = indexOneRow.getEntry(IndexEntry.SIZE*10,IndexEntry.SIZE);
    Assert.assertEquals(entry.getMessageLocation(), 100000123123l);
    entry = new IndexEntry();
    entry.setMessageLength(0);
    entry.setMessageLocation(10012412341324l);
  //  entry.getBuffer().flip();
    entries -= 100;
    indexOneRow.update(IndexEntry.SIZE*(entries ),entry.getBuffer());
    entry = indexOneRow.getEntry(IndexEntry.SIZE*(entries ), IndexEntry.SIZE);
    Assert.assertEquals(entry.getMessageLocation(), 10012412341324l  );
  }

  @Test
  public void testProcessedDeleteorupdate() throws IOException {
    int entries = (int)(OneRow.APPEND_MC_SIZE / IndexEntry.SIZE) + 100;
    indexOneRow.medadata.setProcessedSize(10000);
    insertEntries(entries);
    IndexEntry entry = new IndexEntry();
    entry.setMessageLength(0);
    entry.setMessageLocation(100000123123l);
  //  entry.getBuffer().flip();
    indexOneRow.update(10000 + IndexEntry.SIZE*10, entry.getBuffer());
    entry = indexOneRow.getEntry(10000 + IndexEntry.SIZE*10,IndexEntry.SIZE);
    Assert.assertEquals(entry.getMessageLocation(), 100000123123l);
    entry = new IndexEntry();
    entry.setMessageLength(0);
    entry.setMessageLocation(10012412341324l);
  //  entry.getBuffer().flip();
    entries -= 100;
    indexOneRow.update(10000+ IndexEntry.SIZE*(entries ),entry.getBuffer());
    entry = indexOneRow.getEntry(10000+ IndexEntry.SIZE*(entries ), IndexEntry.SIZE);
    Assert.assertEquals(entry.getMessageLocation(), 10012412341324l  );
  }

}
