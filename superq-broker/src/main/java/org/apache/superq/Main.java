package org.apache.superq;

import java.io.IOException;
import javax.jms.JMSException;


public class Main {

  public static void main(String[] args) throws IOException, JMSException {
    BrokerService brokerService = new BrokerService();
    brokerService.start();
  }

}
