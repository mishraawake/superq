package org.apache.superq.util;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

public class Compressor {
  public static byte[] compress(String uncompress) throws IOException {
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    GZIPOutputStream gzip = new GZIPOutputStream(out);
    gzip.write(uncompress.getBytes());
    gzip.close();
    return out.toByteArray();
  }

  public static String unCompress(byte[] str) throws IOException {
    ByteArrayInputStream is = new ByteArrayInputStream(str);
    byte[] buffer = new byte[1024];
    GZIPInputStream ins = new GZIPInputStream(is);
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    int totalSize = 0;
    while((totalSize = ins.read(buffer)) > 0){
      out.write(buffer, 0, totalSize);
    }
    String output = out.toString();
    out.close();
    ins.close();
    is.close();
    return output;
  }
}
