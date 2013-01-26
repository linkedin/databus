package com.linkedin.databus.core.util;

import java.nio.ByteBuffer;
import java.util.Random;

import com.linkedin.databus.core.DbusEventV1;
import com.linkedin.databus.core.DbusEvent;
import com.linkedin.databus.core.DbusEventKey;
import com.linkedin.databus.core.KeyTypeNotImplementedException;

public class RngUtils {

  public static final String Letters ="ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz";
  public static final String Digits = "0123456789";
  public static final String LettersAndDigits = Letters + Digits;
  /* A consistent random number generator to make tests repeatable */
  public static final Random seededRandom = new Random(192348092834L);
  public static final Random random = new Random();
  public static final byte[] schemaMd5 = "abcdefghijklmnop".getBytes();


  public static String randomString(int length) {
    return randomString(seededRandom, length);
  }

  public static String randomString(Random rng, int length) {
    StringBuilder sb = new StringBuilder();
    for (int i=0; i < length;++i) {
      sb.append(LettersAndDigits.charAt(rng.nextInt(LettersAndDigits.length())));
    }
    return sb.toString();
  }

  public static long randomLong() {
    return seededRandom.nextLong();
  }

  public static long randomPositiveLong() {
    return randomPositiveLong(seededRandom);
  }

  public static long randomPositiveLong(Random rng) {
    long randomLong = rng.nextLong();
    return Long.MIN_VALUE == randomLong ? Long.MAX_VALUE  : Math.abs(randomLong);
  }

  /**
   *
   * @param keyMin
   * @param keyMax
   * @return random long number in range [keyMin, keyMax)
   * Equivalent to [keyMin, keyMax-1]
   */
  public static long randomPositiveLong(long keyMin, long keyMax)
  {
    return randomPositiveLong(seededRandom, keyMin, keyMax);
  }

  public static long randomPositiveLong(Random rng, long keyMin, long keyMax)
  {
    long randomLongOffset = Math.abs(rng.nextLong() % (keyMax-keyMin));
    return (keyMin + randomLongOffset);
  }


  public static short randomPositiveShort() {
    return randomPositiveShort(seededRandom);
  }

  public static short randomPositiveShort(Random rng) {
    short randomShort = (short) (rng.nextInt() & 0x7f);
    return randomShort;
  }

  public static DbusEvent randomEvent(short srcId) {
    ByteBuffer serBuf = ByteBuffer.allocate(1000).order(DbusEventV1.byteOrder);
    try
    {
      DbusEventV1.serializeEvent(new DbusEventKey(randomLong()), (short) 0, // physical Partition
                               randomPositiveShort(), System.currentTimeMillis(), srcId, schemaMd5,
                               randomString(20).getBytes(), (randomPositiveShort()%100 <=1), serBuf);
    }
    catch (KeyTypeNotImplementedException e1)
    {
      throw new RuntimeException(e1);
    }
    serBuf.rewind();
    DbusEventV1 e = new DbusEventV1();
    e.reset(serBuf, serBuf.position());
    return e;
  }

  public static int randomPositiveInt() {
    return randomPositiveInt(seededRandom);
  }

  public static int randomPositiveInt(Random rng) {
    int i = rng.nextInt();
    int randomInt = (i == Integer.MIN_VALUE) ? Integer.MAX_VALUE : Math.abs(i);
    return randomInt;
  }

  public static long randomGaussianLong(long min, long max, Random rng)
  {
    long v = (long)((max + min) * 0.5 + rng.nextGaussian() * (max - min) * 0.5);
    return v < min ? min : (v > max ? max : v);
  }
}
