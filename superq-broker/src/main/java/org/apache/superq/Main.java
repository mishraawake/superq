package org.apache.superq;

import java.io.IOException;
import java.lang.management.ManagementFactory;
import javax.jms.JMSException;
import javax.management.InstanceAlreadyExistsException;
import javax.management.MBeanRegistrationException;
import javax.management.MBeanServer;
import javax.management.MalformedObjectNameException;
import javax.management.NotCompliantMBeanException;
import javax.management.ObjectName;

import org.apache.superq.storage.QueueStats;


public class Main {

  public static void main(String[] args) throws IOException, JMSException, MalformedObjectNameException, NotCompliantMBeanException, InstanceAlreadyExistsException, MBeanRegistrationException {
    BrokerService brokerService = new BrokerService();
    brokerService.start();
  }

}
