package com.linkedin.databus.core.util;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.charset.Charset;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

public class CompressUtil
{
  public static String compress(String str) throws IOException
  {
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    GZIPOutputStream gzip = new GZIPOutputStream(out);
    gzip.write(str.getBytes(Charset.defaultCharset()));
    gzip.close();
    String gzipStr = out.toString("ISO-8859-1");
    return gzipStr;
  }

  public static String uncompress(String str) throws IOException
  {
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    ByteArrayInputStream in = new ByteArrayInputStream(str.getBytes("ISO-8859-1"));
    GZIPInputStream gunzip = new GZIPInputStream(in);
    byte[] buffer = new byte[256];
    int n;
    while ((n = gunzip.read(buffer)) >= 0)
    {
      out.write(buffer, 0, n);
    }
    return out.toString(Charset.defaultCharset().name());
  }
}
