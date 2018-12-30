package org.apache.superq.datatype;

import java.nio.ByteBuffer;
import java.util.Objects;

public class PeerId {
  int port;
  byte iplength;
  String ip;

  public int getPort() {
    return port;
  }

  public void setPort(int port) {
    this.port = port;
  }

  public byte getIplength() {
    return iplength;
  }

  public void setIplength(byte iplength) {
    this.iplength = iplength;
  }

  public String getIp() {
    return ip;
  }

  public void setIp(String ip) {
    this.ip = ip;
  }

  public int getLength(){
    return Integer.BYTES + Byte.BYTES + iplength;
  }

  public int getSize(){
    return Integer.BYTES + Byte.BYTES + ip.length();
  }

  @Override
  public boolean equals(Object o) {
    if (this == o)
      return true;
    if (o == null || getClass() != o.getClass())
      return false;
    PeerId peerId = (PeerId) o;
    return port == peerId.port &&
            iplength == peerId.iplength &&
            Objects.equals(ip, peerId.ip);
  }

  @Override
  public int hashCode() {
    return Objects.hash(port, iplength, ip);
  }

  @Override
  public String toString() {
    return "PeerId{" +
            "port=" + port +
            ", iplength=" + iplength +
            ", ip='" + ip + '\'' +
            '}';
  }
}
