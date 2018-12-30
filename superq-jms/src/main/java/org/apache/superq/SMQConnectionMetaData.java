package org.apache.superq;

import java.util.Enumeration;
import javax.jms.ConnectionMetaData;
import javax.jms.JMSException;

public class SMQConnectionMetaData implements ConnectionMetaData {

  @Override
  public String getJMSVersion() throws JMSException {
    return null;
  }

  @Override
  public int getJMSMajorVersion() throws JMSException {
    return 0;
  }

  @Override
  public int getJMSMinorVersion() throws JMSException {
    return 0;
  }

  @Override
  public String getJMSProviderName() throws JMSException {
    return null;
  }

  @Override
  public String getProviderVersion() throws JMSException {
    return null;
  }

  @Override
  public int getProviderMajorVersion() throws JMSException {
    return 0;
  }

  @Override
  public int getProviderMinorVersion() throws JMSException {
    return 0;
  }

  @Override
  public Enumeration getJMSXPropertyNames() throws JMSException {
    return null;
  }
}
