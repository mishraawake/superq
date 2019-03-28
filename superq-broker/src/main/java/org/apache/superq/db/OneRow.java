package org.apache.superq.db;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.NavigableMap;
import java.util.Queue;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.PriorityBlockingQueue;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.superq.Serialization;
import org.apache.superq.log.Logger;
import org.apache.superq.log.LoggerFactory;

public class OneRow<T extends Serialization> {

  Logger log = LoggerFactory.getLogger(FileDatabase.class);
  RandomAccessFile randomAccessFile;
  MemoryCell memoryCell;
  NavigableMap<Interval, MemoryCell> sortedScattedCell = new ConcurrentSkipListMap<>();
  Queue<MemoryCell> priorityScattedCell = new PriorityBlockingQueue<>();
  String fileName;
  String name;
  FileMetadata medadata;
  private boolean inMemory;
  AtomicLong totalMemoryAllocated = new AtomicLong(APPEND_MC_SIZE);
  public static long APPEND_MC_SIZE = 240 * (1024*1024);//128 MB
  public static long RANDOM_MC_SIZE = 10 *(1024*1024);//10 MB
  public static long TOTAL_MEMORY_SLAB = APPEND_MC_SIZE + RANDOM_MC_SIZE * 800;
  public int totalAccess = 0;
  byte[][] memoryByte = null;
  private SizeableFactory<T> sizeableFactory;

  public OneRow(String dbLocation, String fileName, SizeableFactory<T> sizeableFactory) throws IOException {
    this.sizeableFactory = sizeableFactory;
    this.fileName = fileName;
    fileInitialization(dbLocation, fileName);
    initializeMMCell();
  }


  public void mainInMemory() throws IOException {
    long stime = System.currentTimeMillis();
    inMemory = true;
    memoryByte = new byte[(int)this.medadata.getSize()/20][20];
    MappedByteBuffer mmBuffer = randomAccessFile.getChannel().map(FileChannel.MapMode.READ_ONLY, 0, this.medadata.getSize());
    for(int i=0; i<this.medadata.getSize()/20;++i){
      mmBuffer.get(memoryByte[i]);
    }
    System.out.println("loaded in "+(System.currentTimeMillis() - stime));
  }

  private T getFromMemory(long locationInFile, T sizeable) throws IOException {
    if(inMemory){
      int arrayLocation = (int) (locationInFile / sizeable.getSize());
      sizeable.acceptByteBuffer(memoryByte[arrayLocation]);
      return sizeable;
    }
    return null;
  }

  public boolean isTyped(){
    return this.medadata.getType() == (byte)1;
  }

  public void setTyped(){
     this.medadata.setType((byte)1);
  }

  private void fileInitialization(String dbLocation, String fileName) throws IOException {
    if(!Files.exists(Paths.get(dbLocation)))
      Files.createDirectories(Paths.get(dbLocation));
    medadata = new FileMetadata(dbLocation, fileName+"_");
    File ifile = new File(dbLocation, fileName  );
    createFileIfNotExist(ifile);
    randomAccessFile = new RandomAccessFile(ifile, "rw");
  }

  private void createFileIfNotExist(File file) throws IOException {
    if(!file.exists()){
      if(log.ifInfo())
        log.infoLog("Creating file " +fileName);
      if(!file.createNewFile()){
        throw new IOException("Cannot create file "+ file.getName());
      }
    }
  }

  private synchronized void initializeMMCell() throws IOException {
    memoryCell = initializeAppendMemoryCell(randomAccessFile);
    log.warnLog("Initializing "+this.name+" Append Memory Cell "+memoryCell.interval);
  }

  private synchronized MemoryCell initializeAppendMemoryCell(RandomAccessFile file) throws IOException {
    long indexFileSize = medadata.getSize();
    file.setLength(indexFileSize + APPEND_MC_SIZE);
    Interval interval = new Interval(indexFileSize, indexFileSize + APPEND_MC_SIZE);
    MemoryCell mc = new MemoryCell(interval);
    mc.setMmBuffer(file.getChannel().map(FileChannel.MapMode.READ_WRITE,
                                         mc.getStartIndex(), mc.getEndIndex() - mc.getStartIndex()));
    mc.setAppend(true);
    mc.initialized(interval.getSize());
    return mc;
  }

  private long getNotMemoryMapped(long index, boolean right){
    Interval interval = new Interval(index, index);
    if(right){
      Interval key= sortedScattedCell.ceilingKey(interval);
      if(key != null){

        MemoryCell justRightMemCell = sortedScattedCell.get(key);
        return justRightMemCell.getStartIndex() - index;
      } else {
        return memoryCell.getStartIndex() - index;
      }
    } else {
      Interval key= sortedScattedCell.floorKey(interval);
      if(key != null){
        MemoryCell justLeftMemCell = sortedScattedCell.get(key);
        return index - justLeftMemCell.getEndIndex();
      } else {
        return index;
      }
    }
  }

  private synchronized MemoryCell initializeScatteredMemoryCells(RandomAccessFile file, long index, int size) throws IOException {

    sacrificeMemory();
    long rightSize = getNotMemoryMapped(index, true);
    long leftSize = getNotMemoryMapped(index, false);
    if(rightSize == leftSize){
      throw new RuntimeException("No memory left, right "+index);
    }
    Interval interval = null;
    if(rightSize > RANDOM_MC_SIZE){
      interval = new Interval(index, index + RANDOM_MC_SIZE);
    } else {
      long righSide = index + rightSize;
      long leftSide = 0;
      if(rightSize + leftSize > RANDOM_MC_SIZE){
        leftSide = righSide - RANDOM_MC_SIZE;
      } else {
        leftSide = righSide - (rightSize + leftSize);
      }
      interval = new Interval(leftSide, righSide);
    }
    MemoryCell mc = new MemoryCell(interval);
    mc.setMmBuffer(file.getChannel().map(FileChannel.MapMode.READ_WRITE,
                                         mc.getStartIndex(), mc.getEndIndex() - mc.getStartIndex()));
    mc.setLastValidLocation((int)(interval.getEndIndex() - interval.getStartIndex()));
    totalMemoryAllocated.addAndGet(interval.getSize());
    sortedScattedCell.put(interval, mc);
    priorityScattedCell.add(mc);
    return mc;
  }

  private void sacrificeMemory() {
    if(totalMemoryAllocated.intValue()  + RANDOM_MC_SIZE > TOTAL_MEMORY_SLAB){
      System.out.println("sacrificing memory ..");
      if(priorityScattedCell.size() > 0) {
        MemoryCell cell = priorityScattedCell.poll();
        sortedScattedCell.remove(cell.getInterval());
        totalMemoryAllocated.addAndGet(-cell.getInterval().getSize());
      }
    }
  }

  public long getProcessedSize(){
    return medadata.getProcessedSize();
  }


  public synchronized long append(T entry) throws IOException {
    return append(entry, medadata.getProcessedSize() +  memoryCell.getStartIndex() + memoryCell.getLastValidLocation());
  }

  private boolean doesEntryExists(long location){
    int startingLocation = (int) (location - ( medadata.getProcessedSize() +  memoryCell.getStartIndex() ) );
    long time = memoryCell.getMmBuffer().getLong(startingLocation);
    if(time == 0 ){
      return false;
    }
    return true;
  }

  public synchronized long append(T entry, long location) throws IOException {
    if(isTyped()){
      ByteBuffer bb = ByteBuffer.allocate(2);
      bb.putShort(entry.getType());
      location = appendInternals(bb.array(), location);
      appendInternals(entry.getBuffer(), location + 2);
      return location;
    } else {
      return appendInternals(entry.getBuffer(), location);
    }

  }

  private long appendInternals(byte[] leftOverByte, long location) throws IOException {
    int startingLocation = (int) (location - ( medadata.getProcessedSize() +  memoryCell.getStartIndex() ) );
    if(startingLocation < memoryCell.getLastValidLocation()){
      throw new RuntimeException("Trying to overriding the data. startingLocation " +startingLocation +
                                         " Perhaps messageid creation logic got screwed "+memoryCell.getLastValidLocation());
    }
    int memorySizeLeft = (int)(memoryCell.getInterval().getSize() - startingLocation);

    if( memorySizeLeft < leftOverByte.length ){
      memoryCell.write(leftOverByte, startingLocation, 0, memorySizeLeft);
      medadata.setSize( (location - medadata.getProcessedSize()) + memorySizeLeft  );
      initializeMMCell();
      memoryCell.write(leftOverByte,memorySizeLeft, leftOverByte.length - memorySizeLeft );
    } else {
      memoryCell.write(leftOverByte, startingLocation);
    }
    medadata.setSize((location - medadata.getProcessedSize()) + leftOverByte.length);
   // this.randomAccessFile.getChannel().force(true);
    return location;
  }

  public synchronized void update(long messageId, byte[] byteBuffer) throws IOException {
    if(messageId >= medadata.getProcessedSize() + memoryCell.getStartIndex() + memoryCell.getLastValidLocation()){
      return;
    }
    int sizeLeft = byteBuffer.length;
    int indexSize = 0;
    long locationWF = messageId - medadata.getProcessedSize();
    while(sizeLeft > 0){
      MemoryCell cell = getMemoryCell(locationWF, sizeLeft);
      int sizeToBeUpdated = (int) (cell.getEndIndex() - locationWF);
      if(sizeToBeUpdated > sizeLeft){
        sizeToBeUpdated = sizeLeft;
      }
      cell.update(Arrays.copyOfRange(byteBuffer, indexSize, indexSize + sizeToBeUpdated), locationWF);
      indexSize += sizeToBeUpdated;
      sizeLeft -= sizeToBeUpdated;
      locationWF += sizeToBeUpdated;
    }
  }

  private MemoryCell getMemoryCell(long messageId, int size) throws IOException {
    if(memoryCell.doesExists(messageId, size)){
      return memoryCell;
    } else {
      return searchOrCreateMC(messageId, size);
    }
  }

  public synchronized T getEntry(long actualIndex, int size) throws IOException {
    if(actualIndex >= medadata.getProcessedSize() + memoryCell.getStartIndex() + memoryCell.getLastValidLocation()){
      return null;
    }
    T entry = sizeableFactory.getSizeable();
    ++totalAccess;
    long locationWF = actualIndex - medadata.getProcessedSize();
    if(inMemory){
      return getFromMemory(locationWF, entry);
    }
  //  ByteBuffer byteBuffer = ByteBuffer.allocate(size);
   // byte[] readBytes = getBytes(size, locationWF);
    if(isTyped()){
      ByteBuffer bb = ByteBuffer.allocate(2);
      byte[] sizeBytes = getBytes(2, locationWF);
      bb.put(sizeBytes);
      bb.flip();
      short type = bb.getShort();
      entry = sizeableFactory.getSizeableByType(type);
      locationWF += 2;
      size -= 2;
    } else {
      entry = sizeableFactory.getSizeable();
    }
    entry.acceptByteBuffer(getBytes(size, locationWF));
    return entry;
  }

  private byte[] getBytes(int size, long locationWF) throws IOException {
    byte[] readBytes = new byte[size];
    int sizeLeft = size, offset = 0;
    while(sizeLeft > 0){
      MemoryCell cell = getMemoryCell(locationWF, size);
      int sizeToBeRead = (int) ( cell.getEndIndex() - locationWF);
      if(sizeToBeRead > sizeLeft){
        sizeToBeRead = sizeLeft;
      }
      byte[] bytes =  new byte[sizeToBeRead];
      cell.read(readBytes, locationWF, offset, sizeToBeRead );
      sizeLeft -= sizeToBeRead;
      locationWF += sizeToBeRead;
      offset += sizeToBeRead;
    }
    return readBytes;
  }

  private MemoryCell searchOrCreateMC(long actualIndex, int size) throws IOException {
    Interval interval = new Interval(actualIndex, actualIndex);


    Interval floorKey = sortedScattedCell.floorKey(interval);
    if(floorKey != null){

      MemoryCell justLeftMemCell = sortedScattedCell.get(floorKey);
      if(justLeftMemCell.getEndIndex() > actualIndex && justLeftMemCell.getStartIndex() <= actualIndex){
        return justLeftMemCell;
      }
    }
    long stime = System.currentTimeMillis();
    MemoryCell cell = initializeScatteredMemoryCells(randomAccessFile, actualIndex, size);
    System.out.println("initializing "+totalAccess + " "+name + " in "+(System.currentTimeMillis() - stime));
    return cell;
  }

  public String getName() {
    return name;
  }

  public void setName(String name) {
    this.name = name;
  }

  public static void main(String[] args) throws InterruptedException {

    Queue<MemoryCell> priorityScattedCell = new PriorityBlockingQueue<>();

    NavigableMap<Interval, MemoryCell> sortedScattedCell = new TreeMap<>();
    Interval interval = new Interval(24075540, 25124116);
    sortedScattedCell.put(interval, new MemoryCell(interval));
    priorityScattedCell.add( new MemoryCell(interval));
    Thread.sleep(10);

     interval = new Interval(20, 1048596);
    sortedScattedCell.put(interval, new MemoryCell(interval));
    priorityScattedCell.add( new MemoryCell(interval));
    Thread.sleep(10);
     interval = new Interval(0, 1048576);
    sortedScattedCell.put(interval, new MemoryCell(interval));
    priorityScattedCell.add( new MemoryCell(interval));
    Thread.sleep(10);
    interval = new Interval(2414867322l,2415915898l);
    MemoryCell floor = sortedScattedCell.get(interval);
    priorityScattedCell.add( new MemoryCell(interval));

    Thread.sleep(10);
    System.out.println(floor + " sdfsd" + priorityScattedCell.poll() );
    System.out.println(floor + " sdfsd" + priorityScattedCell.poll() );
    System.out.println(floor + " sdfsd" + priorityScattedCell.poll() );
    System.out.println(floor + " sdfsd" + priorityScattedCell.poll() );
  }
}
