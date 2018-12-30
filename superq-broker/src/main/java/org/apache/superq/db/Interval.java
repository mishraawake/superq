package org.apache.superq.db;

import java.util.Objects;

public class Interval implements Comparable<Interval> {
  private long startIndex;
  private long endIndex;

  public Interval(long startIndex, long endIndex){
    this.startIndex = startIndex;
    this.endIndex = endIndex;
  }

  public long getStartIndex() {
    return startIndex;
  }

  public void setStartIndex(long startIndex) {
    this.startIndex = startIndex;
  }

  public long getEndIndex() {
    return endIndex;
  }

  public void setEndIndex(long endIndex) {
    this.endIndex = endIndex;
  }

  public int getSize(){
    return (int)(endIndex - startIndex);
  }

  @Override
  public int compareTo(Interval o) {
    if(o instanceof Interval){
      Interval other = (Interval)o;
      if(this.getStartIndex() == other.getStartIndex()){
        return 0;
      }
      return (this.getStartIndex() - other.getStartIndex()) > 0 ? 1 : -1;
    }
    return 0;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o)
      return true;
    if (o == null || getClass() != o.getClass())
      return false;
    Interval interval = (Interval) o;
    return startIndex == interval.startIndex &&
            endIndex == interval.endIndex;
  }

  @Override
  public int hashCode() {
    return Objects.hash(startIndex, endIndex);
  }

  @Override
  public String toString() {
    return "Interval{" +
            "startIndex=" + startIndex +
            ", endIndex=" + endIndex +
            '}';
  }
}
