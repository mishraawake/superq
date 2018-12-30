package org.apache.superq;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.sun.org.apache.xpath.internal.operations.Bool;

public abstract class SerializationSupport implements Serialization {

  protected List<Boolean> fieldArray;
  protected int deserializingIndex = -1;
  protected byte[] bytes;

  @Override
  public int getSize() throws IOException {
    return bytes.length;
  }

  @Override
  public byte[] getBuffer() throws IOException {
    initializeBytes();
    return bytes;
  }

  private void initializeBytes() throws IOException {
    if(bytes == null) {
      ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
      DataOutputStream dos = new DataOutputStream(byteArrayOutputStream);
      serializeFields(dos);
      bytes = byteArrayOutputStream.toByteArray();
    }
  }

  @Override
  public void acceptByteBuffer(byte[] inputBytes) throws IOException {
    ByteArrayInputStream byteArrayInputStream = new ByteArrayInputStream(inputBytes);
    DataInputStream dis = new DataInputStream(byteArrayInputStream);
    acceptByteBuffer(dis);
  }

  public void acceptByteBuffer(DataInputStream dis) throws IOException{
    deserializingIndex = -1;
    deserializeField(dis);
  }

  protected void deserializeField(DataInputStream dis) throws IOException {
    short numberOfFields = dis.readShort();
    int numberOfBytes = numberOfFields/8 + (numberOfFields%8 == 0 ? 0 : 1);
    List<Boolean> fieldsExists = new ArrayList<>(numberOfFields);
    for (int fieldByte = 0; fieldByte < numberOfBytes; fieldByte++) {
      for (int bit = 0; bit < 8; bit++) {
        if(fieldsExists.size() >= numberOfFields){
          break;
        }
        fieldsExists.add( ((fieldByte >> bit) & (byte)1) == 1);
      }
    }
  }


  protected void serializeField(DataOutputStream dos) throws IOException {
    byte currentbyte = 0, oneByte = 1;
    int numberOfBits = 0;
    for(Boolean field : fieldArray){
      currentbyte |= oneByte << numberOfBits;
      ++numberOfBits;
      if(numberOfBits == 8){
        dos.write(currentbyte);
        currentbyte = 0;
        numberOfBits = 0;
      }
    }
  }

  protected Map<String, Object> deserializeMap(DataInputStream dis) throws IOException {
    ++deserializingIndex;
    if(fieldArray.get(deserializingIndex)){
      return MapSerializationSupport.unmarshalPrimitiveMap(dis);
    }
    return null;
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
      MapSerializationSupport.marshalPrimitiveMap( field, bb);
    }
  }

  protected void serializeByteString(DataOutputStream bb, String field) throws IOException {
    beforeField(field);
    if(field != null) {
      byte[] utfbytes = field.getBytes(StandardCharsets.UTF_8);
      bb.writeShort((byte)utfbytes.length);
      bb.write(utfbytes);
    }
  }

  protected String deserializeByteString(DataInputStream dis) throws IOException {
    ++deserializingIndex;
    if(fieldArray.get(deserializingIndex)){
      int strLength = dis.readByte();
      byte[] bytes = new byte[strLength];
      dis.read(bytes);
      return new String(bytes, StandardCharsets.UTF_8);
    }
    return null;
  }



  protected void serializeShortString(DataOutputStream bb, String field) throws IOException {
    beforeField(field);
    if(field != null) {
      byte[] utfbytes = field.getBytes(StandardCharsets.UTF_8);
      bb.writeShort((short)utfbytes.length);
      bb.write(utfbytes);
    }
  }

  protected String deserializeShortString(DataInputStream dis) throws IOException {
    ++deserializingIndex;
    if(fieldArray.get(deserializingIndex)){
      int strLength = dis.readShort();
      byte[] bytes = new byte[strLength];
      dis.read(bytes);
      return new String(bytes, StandardCharsets.UTF_8);
    }
    return null;
  }

  protected void serializeIntString(DataOutputStream bb, String field) throws IOException {
    beforeField(field);
    if(field != null) {
      byte[] utfbytes = field.getBytes(StandardCharsets.UTF_8);
      bb.writeInt(utfbytes.length);
      bb.write(utfbytes);
    }
  }


  protected String deserializeIntString(DataInputStream dis) throws IOException {
    ++deserializingIndex;
    if(fieldArray.get(deserializingIndex)){
      int strLength = dis.readInt();
      byte[] bytes = new byte[strLength];
      dis.read(bytes);
      return new String(bytes, StandardCharsets.UTF_8);
    }
    return null;
  }



  protected void serializeLong(DataOutputStream bb, Long field) throws IOException {
    beforeField(field);
    if(field != null) {
      bb.writeLong(field);
    }
  }

  protected Long deSerializeLong(DataInputStream dis) throws IOException {
    ++deserializingIndex;
    if(fieldArray.get(deserializingIndex)){
      return dis.readLong();
    }
    return null;
  }

  protected void serializeInt(DataOutputStream bb, Integer field) throws IOException {
    beforeField( field);
    if(field != null) {
      bb.writeInt(field);
    }
  }

  protected Integer deSerializeInteger(DataInputStream dis) throws IOException {
    ++deserializingIndex;
    if(fieldArray.get(deserializingIndex)){
      return dis.readInt();
    }
    return null;
  }

  protected void serializeShort(DataOutputStream bb, Short field) throws IOException {
    beforeField(field);
    if(field != null) {
      bb.writeShort(field);
    }
  }

  protected Short deSerializeShort(DataInputStream dis) throws IOException {
    ++deserializingIndex;
    if(fieldArray.get(deserializingIndex)){
      return dis.readShort();
    }
    return null;
  }

  protected void serializeByte(DataOutputStream bb, Byte field) throws IOException {
    beforeField(field);
    if(field != null) {
      bb.write(field);
    }
  }

  protected Byte deserializeByte(DataInputStream dis) throws IOException {
    ++deserializingIndex;
    if(fieldArray.get(deserializingIndex)){
      return dis.readByte();
    }
    return null;
  }

  protected void serializeBoolean(DataOutputStream bb, Boolean field) throws IOException {
    beforeField(field);
    if(field != null) {
      bb.write(field ? (byte)1 : (byte)0);
    }
  }

  protected Boolean deSerializeBoolean(DataInputStream dis) throws IOException {
    ++deserializingIndex;
    if(fieldArray.get(deserializingIndex)){
      return dis.readByte() == (byte) 1 ? true:false;
    }
    return null;
  }

}
