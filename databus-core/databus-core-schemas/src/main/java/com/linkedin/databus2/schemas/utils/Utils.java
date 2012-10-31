package com.linkedin.databus2.schemas.utils;

import java.io.File;
import java.io.UnsupportedEncodingException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.List;

/**
 * "Borrowed" largely from com.linkedin.avro.utils.Utils
 */
public class Utils
{

  public static String hex(byte[] bytes)
  {
    StringBuilder builder = new StringBuilder(2 * bytes.length);
    for(int i = 0; i < bytes.length; i++) builder.append(Integer.toHexString(0xFF & bytes[i]));
    return builder.toString();
  }

  public static byte[] md5(byte[] bytes)
  {
    try
    {
      MessageDigest digest = MessageDigest.getInstance("md5");
      return digest.digest(bytes);
    }
    catch(NoSuchAlgorithmException e)
    {
      throw new IllegalStateException("This can't happen.", e);
    }
  }

  public static byte[] utf8(String s)
  {
    try
    {
      return s.getBytes("UTF-8");
    }
    catch(UnsupportedEncodingException e)
    {
      throw new IllegalStateException("This can't happen");
    }
  }

  public static <T> T notNull(T t)
  {
    if(t == null) throw new IllegalArgumentException("Null value not allowed.");
    else return t;
  }

  public static boolean equals(Object x, Object y)
  {
    if(x == y)return true;
    else if(x == null) return false;
    else if(y == null) return false;
    else return x.equals(y);
  }

  public static List<File> toFiles(List<String> strs)
  {
    List<File> files = new ArrayList<File>();
    for(String s: strs) files.add(new File(s));
    return files;
  }

  public static byte[] getBytes(String s, String enc)
  {
    try
    {
      return s.getBytes(enc);
    }
    catch(UnsupportedEncodingException exp)
    {
      throw new RuntimeException(exp);
    }
  }

}
