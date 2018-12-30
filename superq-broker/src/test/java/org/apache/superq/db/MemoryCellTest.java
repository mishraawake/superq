package org.apache.superq.db;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class MemoryCellTest extends AbstractTest {
  MemoryCell memoryCell;
  RandomAccessFile raf;

  @Before
  public void setup(){


  }

  private void initialize() throws IOException {
    for(File f: new File(TEST_DATA_DIRECTORY).listFiles()){
      f.delete();
    }
    raf = new RandomAccessFile(new File(TEST_DATA_DIRECTORY, "mc"), "rw");
    memoryCell = new MemoryCell(new Interval(0, 1024));
    memoryCell.setAppend(true);
    memoryCell.setMmBuffer(raf.getChannel().map(FileChannel.MapMode.READ_WRITE,
                                                memoryCell.getStartIndex(), memoryCell.getEndIndex()));
  }

  @Test
  public void testWrite() throws IOException {
    initialize();
    ByteBuffer bb = ByteBuffer.allocate(10);
    bb.put(("Testing byte buffer".substring(0, 10)).getBytes());
    int validLocation = memoryCell.write(bb);
    Assert.assertEquals(validLocation , 10);
  }

  @Test
  public void doesExistsTest() throws IOException {

    initialize();
    writeIntoMemoryCell(3);
    Assert.assertTrue( memoryCell.doesExists(10, 10) );

    Assert.assertTrue( memoryCell.doesExists(20, 10) );

    Assert.assertFalse( memoryCell.doesExists(30, 10) );

    Assert.assertFalse( memoryCell.doesExists(50, 10) );
  }

  private void writeIntoMemoryCell(int times){
    ByteBuffer bb = null;
    for (int i = 0; i < times; i++) {
      bb = ByteBuffer.allocate(10);
      bb.put(( (i+"Testing byte buffer").substring(0, 10)).getBytes());
      int validLocation = memoryCell.write(bb);
    }
  }

  @Test
  public void readTest() throws IOException {
    initialize();
    writeIntoMemoryCell(3);
    int validLocation = memoryCell.getLastValidLocation();
    byte[] bb = null;
    for (int i = 0; i < 3; i++) {
      bb = new byte[10];
      Assert.assertTrue(memoryCell.read(bb, 10*i));
      Assert.assertEquals(new String(bb), (i+"Testing byte test").substring(0, 10));
    }
    Assert.assertEquals(validLocation, memoryCell.getLastValidLocation());
    bb = new byte[10];
    Assert.assertFalse(memoryCell.read(bb, 40));
  }


  @Test
  public void updateTest() throws IOException {
    byte[] bb = null;
    initialize();
    writeIntoMemoryCell(3);
    int validLocation = memoryCell.getLastValidLocation();
    for (int i = 0; i < 3; i++) {
      String str = "Testing byte buffer"+i;
      bb = (str.substring(str.length() - 10, str.length())).getBytes();
      Assert.assertTrue(memoryCell.update(bb, 10*i));
    }
    bb = ("Testing byte buffer".substring(0,10).getBytes());
    Assert.assertFalse(memoryCell.update(bb, 40));

    for (int i = 0; i < 3; i++) {
      bb = new byte[10];
      Assert.assertTrue(memoryCell.read(bb, 10*i));
      String result = new String(bb);
      String str = "Testing byte buffer"+i;
      String written = str.substring(str.length() - 10, str.length());
      Assert.assertEquals(result, written);
    }

    Assert.assertEquals(validLocation, memoryCell.getLastValidLocation());
  }




}
