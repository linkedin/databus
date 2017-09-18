package com.linkedin.databus.core.util;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.charset.Charset;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

import com.google.common.io.BaseEncoding;

public class CompressUtil
{
  public static String compress(String str) throws IOException
  {
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    GZIPOutputStream gzip = new GZIPOutputStream(out);
    gzip.write(str.getBytes(Charset.defaultCharset()));
    gzip.close();
    return BaseEncoding.base64().encode(out.toByteArray());
  }

  public static String uncompress(String str) throws IOException
  {
	byte[] encodeByteArr = BaseEncoding.base64().decode(str);
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    ByteArrayInputStream in = new ByteArrayInputStream(encodeByteArr);
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
