package org.apache.superq.stat;

public interface RollingMetrics {
  float last5Minute();
  float last10Minute();
  float last15Minute();
  float overall();
}
