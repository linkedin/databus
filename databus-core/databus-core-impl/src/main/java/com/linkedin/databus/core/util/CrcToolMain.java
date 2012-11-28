package com.linkedin.databus.core.util;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;

public class CrcToolMain
{
  private static final int BUFFER_SIZE = 4096;

  private static void printCrc(ByteOrder order, byte[] value)
  {
    ByteBuffer argBuffer = ByteBuffer.wrap(value).order(order);
    long crc = ByteBufferCRC32.getChecksum(argBuffer);
    ByteBuffer resBuffer = ByteBuffer.allocate(4).order(order);
    Utils.putUnsignedInt(resBuffer, 0, crc);

    System.out.print(order.toString() + ": 0x");
    for (int i = 0; i < 4; ++i) System.out.print(Integer.toHexString(resBuffer.get(i) & 0xFF));
  }

  private static void printBothCrcs(String label, byte[] value)
  {
    System.out.print(label + ": ");
    printCrc(ByteOrder.LITTLE_ENDIAN, value);
    System.out.print("     ");
    printCrc(ByteOrder.BIG_ENDIAN, value);
    System.out.println();
  }

  /**
   * @param args
   */
  public static void main(String[] args) throws IOException
  {
    for (String arg: args)
    {
      printBothCrcs(arg, arg.getBytes());
    }

    if (System.in.available() > 0)
    {
      ByteArrayOutputStream baos = new ByteArrayOutputStream();
      byte[] buffer = new byte[BUFFER_SIZE];

      int bytesRead;
      while ((bytesRead = System.in.read(buffer)) > 0)
      {
        baos.write(buffer, 0, bytesRead);
      }
      baos.close();

      byte[] inBytes = baos.toByteArray();
      printBothCrcs("<stdin>", inBytes);
    }
  }

}
