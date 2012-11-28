package com.linkedin.databus.eventgenerator;

/*
 * The interface to generate random values.
 */
public interface RandomDataGenerator {
  public int getNextInt();
  public int getNextInt(int min, int max);
  public String getNextString();
  public String getNextString(int min, int max);
  public double getNextDouble();
  public float getNextFloat();
  public long getNextLong();
  public boolean getNextBoolean();
  public  byte[] getNextBytes(int maxBytesLength);
}
