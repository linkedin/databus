package com.linkedin.databus2.schemas.utils;
/*
 *
 * Copyright 2013 LinkedIn Corp. All rights reserved
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 *
*/


import java.io.File;
import java.io.UnsupportedEncodingException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.codec.binary.Hex;

/**
 * "Borrowed" largely from com.linkedin.avro.utils.Utils
 */
public class Utils
{
  public static final Hex HEX = new Hex();

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
