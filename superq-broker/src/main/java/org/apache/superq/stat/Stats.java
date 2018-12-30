package org.apache.superq.stat;

public interface Stats {
  float max();
  float min();
  float avg();
  float avgPercentile80();
  float avgPercentile90();
  float avgPercentile95();
  float avgPercentile99();
}
