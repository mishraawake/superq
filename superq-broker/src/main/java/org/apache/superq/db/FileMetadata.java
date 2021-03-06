package org.apache.superq.db;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;

import sun.rmi.runtime.Log;

public class FileMetadata {

  MappedByteBuffer mmBuffer;
  int processed = 0;
  int size = processed + Long.BYTES;
  int type = size + Long.BYTES;
  int messageOffset = type + Byte.BYTES;
  int leftOver = messageOffset + Long.BYTES;
  int lastIndex = leftOver + Long.BYTES;


  public FileMetadata(String directory, String fileName) throws IOException {
    RandomAccessFile randomAccessFile = new RandomAccessFile(new File(directory, fileName), "rw");
    mmBuffer = randomAccessFile.getChannel().map(FileChannel.MapMode.READ_WRITE,
                          0, lastIndex);
  }

  public synchronized long getProcessedSize() {
    mmBuffer.position(processed);
    return mmBuffer.getLong();
  }

  public synchronized void setProcessedSize(long processedSize) {
    mmBuffer.position(processed);
    mmBuffer.putLong(processedSize);
  }

  public synchronized long getSize() {
    mmBuffer.position(size);
    return mmBuffer.getLong();
  }

  public synchronized void setSize(long size) {
    mmBuffer.position(this.size);
    mmBuffer.putLong(size);
  }

  public synchronized byte getType() {
    mmBuffer.position(this.type);
    return mmBuffer.get();
  }

  public synchronized void setType(byte size) {
    mmBuffer.position(this.type);
    mmBuffer.put(size);
  }

  public synchronized long messageOffset() {
    mmBuffer.position(this.messageOffset);
    return mmBuffer.getLong();
  }

  public synchronized void messageOffset(long offset) {
    mmBuffer.position(this.messageOffset);
    mmBuffer.putLong(offset);
  }

  public synchronized long leftoverOffset() {
    mmBuffer.position(this.leftOver);
    return mmBuffer.getLong();
  }

  public synchronized void leftoverOffset(long offset) {
    mmBuffer.position(this.leftOver);
    mmBuffer.putLong(offset);
  }

}
