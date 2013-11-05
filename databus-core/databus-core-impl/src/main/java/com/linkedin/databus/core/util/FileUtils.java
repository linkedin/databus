package com.linkedin.databus.core.util;

import java.io.BufferedReader;
import java.io.DataInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;

public class FileUtils
{

  /**
   * Creates a temporary directory
   *
   * @return file object corresponding to the new directory
   * @throws IOException
   */
  public static File createTempDir(String prefix)
      throws IOException
  {
    File dir = File.createTempFile(prefix, null);

    if (!(dir.delete()))
      throw new IOException("Unable to delete temp file " + dir.getAbsolutePath());

    if ( !dir.mkdir())
      throw new IOException("Unable to create tempDir :" + dir.getAbsolutePath());

    dir.deleteOnExit();
    return dir;
  }

  /**
   *
   * Stores the lines passed to this method in the file
   *
   * @param fileName File name to write to
   * @param lines Content lines
   * @throws IOException
   */
  public static void storeLinesToTempFile(String fileName, String[] lines)
      throws IOException
  {
    FileWriter w = new FileWriter(fileName);

    for (String l : lines)
    {
      w.append(l);
      w.append("\n");
    }
    w.close();
  }


  public static void compareTwoTextFiles(String file1, String file2)
    throws AssertionError, IOException
  {
    new FileComparator(file1, file2).compare();
  }


  private static class FileComparator
  {
    private final String _file1;
    private final String _file2;

    public FileComparator(String file1, String file2)
    {
      _file1 = file1;
      _file2 = file2;
    }

    public void compare()
        throws AssertionError, IOException
    {
      BufferedReader br1 = null;
      BufferedReader br2 = null;

      try
      {
        FileInputStream fstream1 = new FileInputStream(_file1);
        FileInputStream fstream2 = new FileInputStream(_file2);

        DataInputStream in1= new DataInputStream(fstream1);
        DataInputStream in2= new DataInputStream(fstream2);

        br1 = new BufferedReader(new InputStreamReader(in1));
        br2 = new BufferedReader(new InputStreamReader(in2));

        String strLine1 = null;
        String strLine2 = null;


        long line = 0;
        while(true)
        {
          strLine1 = br1.readLine();
          strLine2 = br2.readLine();

          if ((strLine1 == null) || (strLine2 == null))
            break;
          
          line++;
          if(! strLine1.equals(strLine2))
          {
            throw new AssertionError("Unmatched : Line :" + line + ", Str 1: (" + strLine1 + "), Str 2: (" + strLine2 + ")");
          }
        }

        if ((strLine1 != null) || (strLine2 != null))
          throw new AssertionError("Unmatched : Line :" + line + ", Str 1: (" + strLine1 + "), Str 2: (" + strLine2 + ")");
      } finally {

        if ( null != br1)
          br1.close();

        if ( null != br2)
          br2.close();
      }
    }
  }
}
