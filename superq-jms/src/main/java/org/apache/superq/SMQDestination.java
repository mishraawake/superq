package org.apache.superq;

import javax.jms.Destination;

public interface SMQDestination extends Destination  {
  int getDestinationId();
}
