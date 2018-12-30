package org.apache.superq.db;

import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.util.Arrays;
import java.util.Date;

public class MemoryCell implements Comparable<MemoryCell> {

  public MemoryCell(Interval interval){
    this.interval = interval;
    touch();
  }

  Interval interval;
  MappedByteBuffer mmBuffer;
  private long lastAccessed;
  private int totalAccess;
  private int lastValidLocation = 0;
  private boolean initialized;
  private boolean append;

  public Interval getInterval() {
    return interval;
  }

  public void setInterval(Interval interval) {
    this.interval = interval;
  }

  public MappedByteBuffer getMmBuffer() {
    return mmBuffer;
  }

  public void setMmBuffer(MappedByteBuffer mmBuffer) {
    this.mmBuffer = mmBuffer;
  }

  public long getLastAccessed() {
    return lastAccessed;
  }

  public void setLastAccessed(long lastAccessed) {
    this.lastAccessed = lastAccessed;
  }

  public int getTotalAccess() {
    return totalAccess;
  }

  public void setTotalAccess(int totalAccess) {
    this.totalAccess = totalAccess;
  }

  private int getSize(){
    return interval.getSize();
  }

  public int sizeLeft(){
    return getSize() - lastValidLocation;
  }

  private void touch(){
    lastAccessed = new Date().getTime();
  }

  public synchronized int write(ByteBuffer bb){
    touch();
    mmBuffer.position(lastValidLocation);
    bb.flip();
    mmBuffer.put(bb.array());
    lastValidLocation += bb.array().length;
    return lastValidLocation;
  }

  public synchronized int write(byte[] bb){
    touch();
    return write(bb, 0, bb.length);
  }

  public synchronized int write(byte[] bb, int location){
    touch();
    return write(bb,location, 0, bb.length);
  }

  public synchronized int write(byte[] bb, int location, int offset, int length){
    touch();
    lastValidLocation = location;
    mmBuffer.position(location);
    mmBuffer.put(bb, offset, length);
    lastValidLocation += length;
    return lastValidLocation;
  }

  public synchronized int write(byte[] bb, int offset, int length){
    touch();
    mmBuffer.position(lastValidLocation);
    mmBuffer.put(bb, offset, length);
    lastValidLocation += length;
    return lastValidLocation;
  }

  public synchronized int write(ByteBuffer bb, int location){
    touch();
    lastValidLocation = location;
    mmBuffer.position(lastValidLocation);
    bb.flip();
    mmBuffer.put(bb);
    lastValidLocation += bb.array().length;
    return lastValidLocation;
  }

  public synchronized boolean doesExists(long location, int size){
    if(location - interval.getStartIndex() >= 0){
      int bufferLocation = (int)(location - interval.getStartIndex());
      if(lastValidLocation >= bufferLocation + size){
        return true;
      }
    }
    return false;
  }

  private boolean getOrUpdate(byte[] bytes, long location, boolean get){
    touch();
    if(location - interval.getStartIndex() >= 0){
      int bufferLocation = (int)(location - interval.getStartIndex());
      if(this.append && lastValidLocation > bufferLocation || !this.append && bufferLocation < getEndIndex()){
        long stime = System.currentTimeMillis();
        mmBuffer.position(bufferLocation);
        //System.out.println("time in seek" + (System.currentTimeMillis() - stime));
        if(get) {
          mmBuffer.get(bytes);
        }
        else
           mmBuffer.put(bytes);
        return true;
      }
    }
    return false;
  }

  private boolean get(byte[] bytes, long location, int offset, int length){
    touch();
    if(location - interval.getStartIndex() >= 0){
      int bufferLocation = (int)(location - interval.getStartIndex());
      if(this.append && lastValidLocation > bufferLocation || !this.append && bufferLocation < getEndIndex()){
        long stime = System.currentTimeMillis();
        mmBuffer.position(bufferLocation);
        //System.out.println("time in seek" + (System.currentTimeMillis() - stime));
        mmBuffer.get(bytes, offset, length);
        return true;
      }
    }
    return false;
  }

  public boolean read(byte[] bb, long location){
    long stime = System.currentTimeMillis();
    boolean result = getOrUpdate(bb, location, true);
   // System.out.println("time in read" + (System.currentTimeMillis() - stime));
    return result;
  }

  public boolean read(byte[] bb, long location, int offset, int length){
    long stime = System.currentTimeMillis();
    boolean result = get(bb, location, offset, length);
    // System.out.println("time in read" + (System.currentTimeMillis() - stime));
    return result;
  }

  public boolean update(byte[] bb, long location){
    return getOrUpdate(bb, location, false);
  }

  public long getStartIndex() {
    return interval.getStartIndex();
  }

  public long getEndIndex() {
    return interval.getEndIndex();
  }

  public int getLastValidLocation() {
    return lastValidLocation;
  }

  public void setLastValidLocation(int lastValidLocation) {
    this.lastValidLocation = lastValidLocation;
  }

  public boolean isInitialized() {
    return initialized;
  }

  public void setInitialized(boolean initialized) {
    this.initialized = initialized;
  }

  public boolean isAppend() {
    return append;
  }

  public void setAppend(boolean append) {
    this.append = append;
  }

  public synchronized void initialized(int size){
    if(!initialized){
      ByteBuffer bb = ByteBuffer.allocate(size);
      Arrays.fill(bb.array(), (byte)0);
      mmBuffer.put(bb);
    }
  }

  @Override
  public int compareTo(MemoryCell o) {
    if(o instanceof MemoryCell){
      MemoryCell mc = (MemoryCell)o;
      if(this.lastAccessed == mc.lastAccessed){
        return 0;
      }
      return this.lastAccessed - mc.lastAccessed > 0 ? 1: -1;
    }
    return 0;
  }

  @Override
  public String toString() {
    return "MemoryCell{" +
            "interval=" + interval +
            ", lastAccessed=" + lastAccessed +
            ", totalAccess=" + totalAccess +
            ", lastValidLocation=" + lastValidLocation +
            ", initialized=" + initialized +
            ", append=" + append +
            '}';
  }
}
