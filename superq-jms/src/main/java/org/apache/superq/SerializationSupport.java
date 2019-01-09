package org.apache.superq;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.sun.org.apache.xpath.internal.operations.Bool;

public abstract class SerializationSupport implements Serialization {

  protected List<Boolean> fieldArray = new ArrayList<>();
  protected int deserializingIndex = -1;
  protected byte[] bytes;

  @Override
  public int getSize() throws IOException {
    initializeBytes();
    return bytes.length;
  }

  @Override
  public byte[] getBuffer() throws IOException {
    initializeBytes();
    return bytes;
  }

  protected void initializeBytes() throws IOException {
    if(bytes == null) {
      ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
      DataOutputStream dos = new DataOutputStream(byteArrayOutputStream);
      serializeFields(dos);
      bytes = byteArrayOutputStream.toByteArray();
      fillField(bytes);
    }
  }

  @Override
  public void acceptByteBuffer(byte[] inputBytes) throws IOException {
    ByteArrayInputStream byteArrayInputStream = new ByteArrayInputStream(inputBytes);
    DataInputStream dis = new DataInputStream(byteArrayInputStream);
    acceptByteBuffer(dis, getFieldsSize());
    fieldArray = new ArrayList<>();
  }

  public void acceptByteBuffer(DataInputStream dis) throws IOException{
    deserializingIndex = -1;
    deSerializeFields(dis);
  }

  public void acceptByteBuffer(DataInputStream dis, int fields) throws IOException{
    deserializingIndex = -1;
    deSerializeFields(dis);
  }

  public void deSerializeFields(DataInputStream dis) throws IOException {
    int numberOfFields = getFieldsSize();
    int numberOfBytes = numberOfFields/8 + (numberOfFields%8 == 0 ? 0 : 1);
    this.fieldArray = new ArrayList<>(numberOfFields);
    for (int fieldByte = 0; fieldByte < numberOfBytes; fieldByte++) {
      byte currentByte = dis.readByte();
      for (int bit = 0; bit < 8; bit++) {
        int counter = fieldByte * 8 + bit;
        fieldArray.add( ((currentByte >>> bit) & (byte)1) == 1);
      }
    }
  }


  public void serializeFields(DataOutputStream dos) throws IOException {
    int numberOfFields = getFieldsSize();
    byte[] fieldBytes =  new byte[numberOfFields/8];
    dos.write(fieldBytes);
  }


  public void fillField(byte[] bytes) throws IOException {
    if(fieldArray == null || fieldArray.size() == 0){
      return;
    }
    byte currentbyte = 0, oneByte = 1, numberOfBytes = 0;
    int numberOfBits = 0;
    for(Boolean field : fieldArray){
      if(field){
        currentbyte =  (byte)(currentbyte |  1 << numberOfBits);
      }
      ++numberOfBits;
      if(numberOfBits == 8){
        bytes[numberOfBytes++] = currentbyte;
        //dos.write(currentbyte);
        currentbyte = 0;
        numberOfBits = 0;
      }
    }
    if(numberOfBits != 0){
      bytes[numberOfBytes++] = currentbyte;
    }
  }

  protected Map<String, Object> deserializeMap(DataInputStream dis) throws IOException {
    incrementIndex();
    if(fieldArray.get(deserializingIndex)){
      return doGetMap(dis);
    }
    return null;
  }

  protected Map<String, Object> doGetMap(DataInputStream dis) throws IOException {
    return MapSerializationSupport.unmarshalPrimitiveMap(dis);
  }

  private void beforeField( Object field){
    if(field != null) {
      fieldArray.add(true);
    } else {
      fieldArray.add(false);
    }
  }

  protected void serializeMap(DataOutputStream bb, HashMap<String, Object> field) throws IOException {
    beforeField(field);
    if(field != null) {
      doSerializeMap(bb, field);
      return;
    }
  }

  protected void doSerializeMap(DataOutputStream bb, HashMap<String, Object> field) throws IOException {
    MapSerializationSupport.marshalPrimitiveMap(field, bb);
  }

  protected void serializeByteString(DataOutputStream bb, String field) throws IOException {
    beforeField(field);
    if(field != null) {
      doSerializeByteString(bb, field);
    }
  }

  protected void doSerializeByteString(DataOutputStream bb, String field) throws IOException {
    byte[] utfbytes = field.getBytes(StandardCharsets.UTF_8);
    bb.write((byte)utfbytes.length);
    bb.write(utfbytes);
  }

  protected String deserializeByteString(DataInputStream dis) throws IOException {
    incrementIndex();
    if(fieldArray.get(deserializingIndex)){
      return doGetByteString(dis);
    }
    return null;
  }

  protected String doGetByteString(DataInputStream dis) throws IOException {
    int strLength = dis.readByte();
    byte[] bytes = new byte[strLength];
    dis.read(bytes);
    return new String(bytes, StandardCharsets.UTF_8);
  }


  protected void serializeShortString(DataOutputStream bb, String field) throws IOException {
    beforeField(field);
    if(field != null) {
      doSerializeShortString(bb, field);
    }
  }

  protected void doSerializeShortString(DataOutputStream bb, String field) throws IOException {
    byte[] utfbytes = field.getBytes(StandardCharsets.UTF_8);
    bb.writeShort((short)utfbytes.length);
    bb.write(utfbytes);
  }

  protected String deserializeShortString(DataInputStream dis) throws IOException {
    incrementIndex();
    if(fieldArray.get(deserializingIndex)){
      return doGetShortString(dis);
    }
    return null;
  }

  protected String doGetShortString(DataInputStream dis) throws IOException {
    int strLength = dis.readShort();
    byte[] bytes = new byte[strLength];
    dis.read(bytes);
    return new String(bytes, StandardCharsets.UTF_8);
  }

  protected void serializeIntString(DataOutputStream bb, String field) throws IOException {
    beforeField(field);
    if(field != null) {
      doSerializeIntString(bb, field);
    }
  }

  protected void doSerializeIntString(DataOutputStream bb, String field) throws IOException {
    byte[] utfbytes = field.getBytes(StandardCharsets.UTF_8);
    bb.writeInt(utfbytes.length);
    bb.write(utfbytes);
  }


  protected String deserializeIntString(DataInputStream dis) throws IOException {
    incrementIndex();
    if(fieldArray.get(deserializingIndex)){
      return doGetIntString(dis);
    }
    return null;
  }

  protected String doGetIntString(DataInputStream dis) throws IOException {
    int strLength = dis.readInt();
    byte[] bytes = new byte[strLength];
    dis.read(bytes);
    return new String(bytes, StandardCharsets.UTF_8);
  }


  protected void serializeLong(DataOutputStream bb, Long field) throws IOException {
    beforeField(field);
    if(field != null) {
      doSerializeLong(bb, field);
    }
  }

  protected void doSerializeLong(DataOutputStream bb, Long field) throws IOException {
    bb.writeLong(field);
  }

  protected Long deSerializeLong(DataInputStream dis) throws IOException {
    incrementIndex();
    if(fieldArray.get(deserializingIndex)){
      return doGetLong(dis);
    }
    return null;
  }

  protected Long doGetLong(DataInputStream dis) throws IOException {
    return dis.readLong();
  }

  protected void serializeInt(DataOutputStream bb, Integer field) throws IOException {
    beforeField( field);
    if(field != null) {
      doSerializeInteger(bb, field);
    }
  }

  protected void doSerializeInteger(DataOutputStream bb, Integer field) throws IOException {
    bb.writeInt(field);
  }

  protected Integer deSerializeInteger(DataInputStream dis) throws IOException {
    incrementIndex();
    if(fieldArray.get(deserializingIndex)){
      return doGetInteger(dis);
    }
    return null;
  }

  protected Integer doGetInteger(DataInputStream dis) throws IOException {
    return dis.readInt();
  }

  protected void serializeShort(DataOutputStream bb, Short field) throws IOException {
    beforeField(field);
    if(field != null) {
      doSerializeShort(bb, field);
    }
  }

  protected void doSerializeShort(DataOutputStream bb, Short field) throws IOException {
    bb.writeShort(field);
  }

  protected Short deSerializeShort(DataInputStream dis) throws IOException {
    incrementIndex();
    if(fieldArray.get(deserializingIndex)){
      return doGetShort(dis);
    }
    return null;
  }

  protected Short doGetShort(DataInputStream dis) throws IOException {
    return dis.readShort();
  }

  protected void serializeByte(DataOutputStream bb, Byte field) throws IOException {
    beforeField(field);
    if(field != null) {
      doSerializeByte(bb, field);
    }
  }

  protected void doSerializeByte(DataOutputStream bb, Byte field) throws IOException {
    bb.write(field);
  }

  protected Byte deserializeByte(DataInputStream dis) throws IOException {
    incrementIndex();
    if(fieldArray.get(deserializingIndex)){
      return doGetByte(dis);
    }
    return null;
  }

  protected Byte doGetByte(DataInputStream dis) throws IOException {
    return dis.readByte();
  }

  protected void serializeByteArray(DataOutputStream bb, byte[] field) throws IOException {
    beforeField(field);
    if(field != null) {

      doSerializeByteArray(bb, field);
    }
  }

  protected void doSerializeByteArray(DataOutputStream bb, byte[] field) throws IOException {
    bb.writeInt(field.length);
    bb.write(field);
  }

  protected byte[] deserializeByteArray(DataInputStream dis) throws IOException {
    incrementIndex();
    if(fieldArray.get(deserializingIndex)){
      return doGetByteArray(dis);
    }
    return null;
  }

  protected byte[] doGetByteArray(DataInputStream dis) throws IOException {
    int byteLength = dis.readInt();
    byte[] bytes = new byte[byteLength];
    dis.read(bytes);
    return bytes;
  }

  protected void serializeBoolean(DataOutputStream bb, Boolean field) throws IOException {
    beforeField(field);
    if(field != null) {
      doSerializeBoolean(bb, field);
    }
  }

  protected void doSerializeBoolean(DataOutputStream bb, Boolean field) throws IOException {
    bb.write(field ? (byte)1 : (byte)0);
  }

  private void incrementIndex(){
    ++deserializingIndex;
    if(fieldArray.size() <= deserializingIndex){
      throw new RuntimeException("This class needs more bits in getFieldsSize, currently it is "+getFieldsSize());
    }
  }

  protected Boolean deSerializeBoolean(DataInputStream dis) throws IOException {
    incrementIndex();
    if(fieldArray.get(deserializingIndex)){
      return doGetBoolean(dis);
    }
    return null;
  }

  protected Boolean doGetBoolean(DataInputStream dis) throws IOException {
    return dis.readByte() == (byte) 1 ? true:false;
  }

}
