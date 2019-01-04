package org.apache.superq;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Vector;
import javax.jms.DeliveryMode;
import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.Message;

public abstract class SMQMessage extends SerializationSupport implements Message {

  private String jmsMessageId;
  private Long jmsMessageLongId;
  private Long jmsTimestamp;
  private String jmsCorrelationID;
  private Destination jmsReplyTo;
  private Destination jmsDestination;
  private Integer destinationId;
  private Integer replytoDestinationId;
  private Byte jmsDeliveryMode;
  private boolean jmsRedelivered;
  private String jmsType;
  private Long jmsExpiration;
  private Long jmsDeliveryTime;
  private Integer jmsPriority;
  private long producerId;
  private long consumerId;
  private long sessionId;
  private long connectionId;
  private boolean responseRequire;
  Map<String, Object> properties = new HashMap<>();
  private final String GROUP_HEADER_NAME = "JMSXGroupID";
  private final String GROUP_SEQ_HEADER_NAME = "JMSXGroupSeq";
  private transient long transactionId;


  @Override
  public String getJMSMessageID() throws JMSException {
    return this.jmsMessageId;
  }

  @Override
  public void setJMSMessageID(String id) throws JMSException {
    this.jmsMessageId = id;
  }

  @Override
  public long getJMSTimestamp() throws JMSException {
    return this.jmsTimestamp;
  }

  @Override
  public void setJMSTimestamp(long timestamp) throws JMSException {
    this.jmsTimestamp = timestamp;
  }

  @Override
  public byte[] getJMSCorrelationIDAsBytes() throws JMSException {
    return this.jmsCorrelationID.getBytes(StandardCharsets.UTF_8);
  }

  @Override
  public void setJMSCorrelationIDAsBytes(byte[] correlationID) throws JMSException {
    this.jmsCorrelationID = new String(correlationID, StandardCharsets.UTF_8);
  }

  @Override
  public void setJMSCorrelationID(String correlationID) throws JMSException {
    this.jmsCorrelationID = correlationID;
  }

  @Override
  public String getJMSCorrelationID() throws JMSException {
    return this.jmsCorrelationID;
  }

  @Override
  public Destination getJMSReplyTo() throws JMSException {
    return jmsReplyTo;
  }

  @Override
  public void setJMSReplyTo(Destination replyTo) throws JMSException {
    this.jmsReplyTo = replyTo;
  }

  @Override
  public Destination getJMSDestination() throws JMSException {
    return this.jmsDestination;
  }

  @Override
  public void setJMSDestination(Destination destination) throws JMSException {
    this.jmsDestination = destination;
  }

  @Override
  public int getJMSDeliveryMode() throws JMSException {
    return this.jmsDeliveryMode;
  }

  @Override
  public void setJMSDeliveryMode(int deliveryMode) throws JMSException {
    this.jmsDeliveryMode = (byte)deliveryMode;
  }

  @Override
  public boolean getJMSRedelivered() throws JMSException {
    return this.jmsRedelivered;
  }

  @Override
  public void setJMSRedelivered(boolean redelivered) throws JMSException {
    this.jmsRedelivered = redelivered;
  }

  @Override
  public String getJMSType() throws JMSException {
    return this.jmsType;
  }

  @Override
  public void setJMSType(String type) throws JMSException {
    this.jmsType = type;
  }

  @Override
  public long getJMSExpiration() throws JMSException {
    return this.jmsExpiration;
  }

  @Override
  public void setJMSExpiration(long expiration) throws JMSException {
    this.jmsExpiration = expiration;
  }

  @Override
  public long getJMSDeliveryTime() throws JMSException {
    return this.jmsDeliveryTime;
  }

  @Override
  public void setJMSDeliveryTime(long deliveryTime) throws JMSException {
    this.jmsDeliveryTime = deliveryTime;
  }

  @Override
  public int getJMSPriority() throws JMSException {
    return this.jmsPriority;
  }

  @Override
  public void setJMSPriority(int priority) throws JMSException {
    this.jmsPriority = priority;
  }

  public boolean isResponseRequire() {
    return responseRequire;
  }

  public void setResponseRequire(boolean responseRequire) {
    this.responseRequire = responseRequire;
  }

  @Override
  public void clearProperties() throws JMSException {
    properties.clear();
  }

  @Override
  public boolean propertyExists(String name) throws JMSException {
    return properties.containsKey(name);
  }

  @Override
  public boolean getBooleanProperty(String name) throws JMSException {
    return (boolean)properties.get(name);
  }

  @Override
  public byte getByteProperty(String name) throws JMSException {
    return (byte)properties.get(name);
  }

  @Override
  public short getShortProperty(String name) throws JMSException {
    return (short)properties.get(name);
  }

  @Override
  public int getIntProperty(String name) throws JMSException {
    return (int)properties.get(name);
  }

  @Override
  public long getLongProperty(String name) throws JMSException {
    return (long)properties.get(name);
  }

  @Override
  public float getFloatProperty(String name) throws JMSException {
    return (float)properties.get(name);
  }

  @Override
  public double getDoubleProperty(String name) throws JMSException {
    return (double)properties.get(name);
  }

  @Override
  public String getStringProperty(String name) throws JMSException {
    return (String)properties.get(name);
  }

  @Override
  public Object getObjectProperty(String name) throws JMSException {
    return properties.get(name);
  }

  @Override
  public Enumeration getPropertyNames() throws JMSException {
    Vector<String> result = new Vector<String>(this.properties.keySet());
    return result.elements();
  }

  @Override
  public void setBooleanProperty(String name, boolean value) throws JMSException {
    properties.put(name, value);
  }

  @Override
  public void setByteProperty(String name, byte value) throws JMSException {
    properties.put(name, value);
  }

  @Override
  public void setShortProperty(String name, short value) throws JMSException {
    properties.put(name, value);
  }

  @Override
  public void setIntProperty(String name, int value) throws JMSException {
    properties.put(name, value);
  }

  @Override
  public void setLongProperty(String name, long value) throws JMSException {
    properties.put(name, value);
  }

  @Override
  public void setFloatProperty(String name, float value) throws JMSException {
    properties.put(name, value);
  }

  @Override
  public void setDoubleProperty(String name, double value) throws JMSException {
    properties.put(name, value);
  }

  @Override
  public void setStringProperty(String name, String value) throws JMSException {
    properties.put(name, value);
  }

  @Override
  public void setObjectProperty(String name, Object value) throws JMSException {
    properties.put(name, value);
  }

  public long getProducerId() {
    return producerId;
  }

  public void setProducerId(long producerId) {
    this.producerId = producerId;
  }

  public long getConsumerId() {
    return consumerId;
  }

  public void setConsumerId(long consumerId) {
    this.consumerId = consumerId;
  }

  public long getSessionId() {
    return sessionId;
  }

  public void setSessionId(long sessionId) {
    this.sessionId = sessionId;
  }

  public long getConnectionId() {
    return connectionId;
  }

  public void setConnectionId(long connectionId) {
    this.connectionId = connectionId;
  }


  public Long getJmsMessageLongId() {
    return jmsMessageLongId;
  }

  public void setJmsMessageLongId(Long jmsMessageLongId) {
    this.jmsMessageLongId = jmsMessageLongId;
  }

  public String getGroupId(){
    return (String)properties.get(GROUP_HEADER_NAME);
  }

  public void setGroupId(String groupId){
    properties.put(GROUP_HEADER_NAME, groupId);
  }

  public Integer getGroupSeqId(){
    return (Integer) properties.get(GROUP_SEQ_HEADER_NAME);
  }

  public void setGroupSegId(int groupSeqId){
    properties.put(GROUP_SEQ_HEADER_NAME, groupSeqId);
  }

  public long getTransactionId() {
    return transactionId;
  }

  public void setTransactionId(long transactionId) {
    this.transactionId = transactionId;
  }

  @Override
  public void acknowledge() throws JMSException {

  }

  @Override
  public void clearBody() throws JMSException {

  }

  @Override
  public <T> T getBody(Class<T> c) throws JMSException {
    return null;
  }

  @Override
  public boolean isBodyAssignableTo(Class c) throws JMSException {
    return false;
  }

  public byte[] getMessageBytes() throws JMSException {
    try {
      return getBuffer();
    } catch (IOException ioe){
      JMSException jmsException = new JMSException("Error in getting message bytes");
      jmsException.setLinkedException(ioe);
      throw jmsException;
    }
  }



  public void serializeFields(DataOutputStream dos) throws IOException{
    super.serializeFields(dos);
    serializeLong(dos, jmsMessageLongId);
    serializeLong(dos, jmsTimestamp );
    serializeByteString(dos, jmsCorrelationID);
    if(jmsDestination != null)
      destinationId = ((SMQDestination)jmsDestination).getDestinationId();
    if(jmsReplyTo != null)
      replytoDestinationId = ((SMQDestination)jmsReplyTo).getDestinationId();
    serializeInt(dos, destinationId );
    serializeInt(dos, replytoDestinationId );
    serializeByte(dos, jmsDeliveryMode);
    serializeBoolean(dos, jmsRedelivered);
    serializeShortString(dos, jmsType);
    serializeLong(dos, jmsExpiration);
    serializeLong(dos, jmsDeliveryTime );
    serializeLong(dos, producerId );
    serializeLong(dos, consumerId);
    serializeLong(dos, sessionId);
    serializeLong(dos, connectionId);
    serializeBoolean(dos, responseRequire);
    if(jmsPriority != null){
      byte priority = jmsPriority.byteValue();
      serializeByte(dos, priority == 0 ? null: priority);
    } else {
      serializeByte(dos, null);
    }
    serializeMap(dos, (HashMap<String, Object>) properties);
  }
  public void deSerializeFields(DataInputStream dis) throws IOException{
    super.deSerializeFields(dis);
    jmsMessageLongId = deSerializeLong(dis);
    jmsTimestamp = deSerializeLong(dis);
    jmsCorrelationID = deserializeByteString(dis);
    destinationId = deSerializeInteger(dis);
    if(destinationId != null){
      QueueInfo queueInfo = new QueueInfo();
      queueInfo.setId(destinationId);
      jmsDestination = queueInfo;
    }
    replytoDestinationId = deSerializeInteger(dis);
    if(replytoDestinationId != null){
      QueueInfo queueInfo = new QueueInfo();
      queueInfo.setId(replytoDestinationId);
      jmsReplyTo = queueInfo;
    }
    jmsDeliveryMode = deserializeByte(dis);
    jmsRedelivered = deSerializeBoolean(dis);
    jmsType = deserializeByteString(dis);
    jmsExpiration = deSerializeLong(dis);
    jmsDeliveryTime = deSerializeLong(dis);
    producerId = deSerializeLong(dis);
    consumerId = deSerializeLong(dis);
    sessionId = deSerializeLong(dis);
    connectionId = deSerializeLong(dis);
    responseRequire = deSerializeBoolean(dis);
    Byte priorityByte = deserializeByte(dis);
    if(priorityByte != null)
      jmsPriority = priorityByte.intValue();
    properties = deserializeMap(dis);
  }

  public boolean isPersistent() {
    return jmsDeliveryMode == DeliveryMode.PERSISTENT;
  }

  public int getFieldsSize(){
    return 3*8;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o)
      return true;
    if (o == null || getClass() != o.getClass())
      return false;
    SMQMessage message = (SMQMessage) o;
    return jmsRedelivered == message.jmsRedelivered &&
            producerId == message.producerId &&
            consumerId == message.consumerId &&
            sessionId == message.sessionId &&
            connectionId == message.connectionId &&
            responseRequire == message.responseRequire &&
            transactionId == message.transactionId &&
            Objects.equals(jmsMessageId, message.jmsMessageId) &&
            Objects.equals(jmsMessageLongId, message.jmsMessageLongId) &&
            Objects.equals(jmsTimestamp, message.jmsTimestamp) &&
            Objects.equals(jmsCorrelationID, message.jmsCorrelationID) &&
            Objects.equals(destinationId, message.destinationId) &&
            Objects.equals(replytoDestinationId, message.replytoDestinationId) &&
            Objects.equals(jmsDeliveryMode, message.jmsDeliveryMode) &&
            Objects.equals(jmsType, message.jmsType) &&
            Objects.equals(jmsExpiration, message.jmsExpiration) &&
            Objects.equals(jmsDeliveryTime, message.jmsDeliveryTime) &&
            Objects.equals(jmsPriority, message.jmsPriority) &&
            Objects.equals(properties, message.properties) &&
            Objects.equals(GROUP_HEADER_NAME, message.GROUP_HEADER_NAME) &&
            Objects.equals(GROUP_SEQ_HEADER_NAME, message.GROUP_SEQ_HEADER_NAME);
  }

  @Override
  public int hashCode() {
    return Objects.hash(jmsMessageId, jmsMessageLongId, jmsTimestamp, jmsCorrelationID, destinationId, replytoDestinationId, jmsDeliveryMode, jmsRedelivered, jmsType, jmsExpiration, jmsDeliveryTime, jmsPriority, producerId, consumerId, sessionId, connectionId, responseRequire, properties, GROUP_HEADER_NAME, GROUP_SEQ_HEADER_NAME, transactionId);
  }

  @Override
  public String toString() {
    return " jmsMessageId='" + jmsMessageId + '\'' +
            ", jmsMessageLongId=" + jmsMessageLongId +
            ", jmsTimestamp=" + jmsTimestamp +
            ", jmsCorrelationID='" + jmsCorrelationID + '\'' +
            ", jmsReplyTo=" + jmsReplyTo +
            ", jmsDestination=" + jmsDestination +
            ", destinationId=" + destinationId +
            ", replytoDestinationId=" + replytoDestinationId +
            ", jmsDeliveryMode=" + jmsDeliveryMode +
            ", jmsRedelivered=" + jmsRedelivered +
            ", jmsType='" + jmsType + '\'' +
            ", jmsExpiration=" + jmsExpiration +
            ", jmsDeliveryTime=" + jmsDeliveryTime +
            ", jmsPriority=" + jmsPriority +
            ", producerId=" + producerId +
            ", consumerId=" + consumerId +
            ", sessionId=" + sessionId +
            ", connectionId=" + connectionId +
            ", responseRequire=" + responseRequire +
            ", properties=" + properties +
            ", transactionId=" + transactionId;
  }
}
