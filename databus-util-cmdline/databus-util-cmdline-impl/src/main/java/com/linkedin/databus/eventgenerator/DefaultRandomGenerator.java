package com.linkedin.databus.eventgenerator;

import java.util.Random;

public class DefaultRandomGenerator implements RandomDataGenerator
{

  Random rand;
  public String validChars = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789"; //.,/';\\][=-`<>?\":|}{+_)(*&^%$#@!~";

  public DefaultRandomGenerator() {
    rand = new Random();
  }

  public int getNextInt()
  {
    //return getNextInt(IntegerFieldGenerate.getMinIntLength(), IntegerFieldGenerate.getMaxIntLength());
    return getNextInt(0, IntegerFieldGenerate.getMaxIntLength());
  }
  

  public int getNextInt(int min, int max)
  {
    if( max == min ) return min;

    int range = max - min;
    range = range > 0 ? range: -range; //Prevent integer overflow.
    int generated = min + rand.nextInt(range);
    generated = generated > max ? max: generated;
    return min + rand.nextInt(range);
  }

  public String getNextString()
  {
    return getNextString(StringFieldGenerate.minStringLength, StringFieldGenerate.maxStringLength);
  }

  public String getNextString(int min, int max)
  {     
    int length = getNextInt(min, max);

    StringBuffer strbld = new StringBuffer();
    for( int i = 0; i < length; i++)
    {
      char ch = validChars.charAt(rand.nextInt(validChars.length()));
      strbld.append(ch);
    }

    return strbld.toString();
  }

  public double getNextDouble() {
    return rand.nextDouble();
  }

  public float getNextFloat() {
    return rand.nextFloat();
  }

  public long getNextLong() {
    return Math.abs(rand.nextLong());
  }

  public boolean getNextBoolean()
  {
    return rand.nextBoolean();
  }

  public  byte[] getNextBytes(int maxBytesLength)
  {
    byte[] bytes = new byte[this.getNextInt(0, maxBytesLength)];
    rand.nextBytes(bytes);
    return bytes;
  }
}
