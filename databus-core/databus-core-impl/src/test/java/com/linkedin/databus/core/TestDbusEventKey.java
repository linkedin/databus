package com.linkedin.databus.core;


import static org.testng.AssertJUnit.*;

import java.nio.charset.Charset;
import org.testng.annotations.Test;

/**
 * Created with IntelliJ IDEA. User: ssubrama Date: 9/20/12 Time: 3:30 PM To change this template use File | Settings |
 * File Templates.
 */
public class TestDbusEventKey
{

  private long LONG_KEY = 1234L;
  private byte[] BYTES_KEY = {-127, 0, 0, 0};
  private String STRING_KEY = "SomeStringKey";

  private DbusEventKey lkey = new DbusEventKey(LONG_KEY);
  private DbusEventKey bkey = new DbusEventKey(BYTES_KEY);
  private DbusEventKey skey = new DbusEventKey(STRING_KEY);
  private DbusEventKey sbkey = new DbusEventKey(new String(STRING_KEY).getBytes());

  private boolean verifyCannotGetBytes(DbusEventKey key)
  {
    boolean gotException = false;
    try
    {
      key.getStringKeyInBytes();
    }
    catch(RuntimeException e)
    {
      gotException = true;
    }

    return gotException;
  }

  private boolean verifyCannotGetString(DbusEventKey key)
  {
    boolean gotException = false;
    try
    {
      key.getStringKey();
    }
    catch(RuntimeException e)
    {
      gotException = true;
    }

    return gotException;
  }

  @Test
  public void testLongKey()
  {
    assertEquals(lkey.getLongKey(), new Long(LONG_KEY));
    assertEquals(lkey.getKeyType(), DbusEventKey.KeyType.LONG);
    assertTrue(verifyCannotGetBytes(lkey));
    assertTrue(verifyCannotGetString(lkey));
  }

  @Test
  public void testBytesKey()
  {
    assertEquals(bkey.getKeyType(), DbusEventKey.KeyType.STRING);
    assertEquals(bkey.getStringKeyInBytes(), BYTES_KEY);
    assertFalse(verifyCannotGetBytes(bkey));
    assertTrue(verifyCannotGetString(bkey));
  }

  /**
   * @deprecated Remove this test when all constructors of DbusEventKey with String are removed
   * from the code.
   */
  @Test
  public void testStringKey()
  {
    assertEquals(skey.getKeyType(), DbusEventKey.KeyType.STRING);

    assertEquals(skey.getStringKey(), STRING_KEY);
    assertFalse(verifyCannotGetBytes(skey));
    assertFalse(verifyCannotGetString(skey));

    String s1 = STRING_KEY;
    assertEquals(s1.getBytes(Charset.forName("UTF-8")), skey.getStringKeyInBytes());
    DbusEventKey skey1 = new DbusEventKey(s1);
    assertEquals(skey, skey1);
  }

  @Test
  public void testEquality()
  {
    DbusEventKey key;

    key = new DbusEventKey(LONG_KEY);
    assertEquals(lkey, key);
    key = new DbusEventKey(LONG_KEY+1);
    assertNotSame(lkey, key);

    key = new DbusEventKey(STRING_KEY);
    assertEquals(key, skey);
    key = new DbusEventKey(STRING_KEY + "a");
    assertNotSame(key, skey);

    key = new DbusEventKey(BYTES_KEY);
    assertEquals(bkey, key);
    byte[] bytes = {1, 0, 0};
    key = new DbusEventKey(bytes);
    assertNotSame(key, bkey);

    // Even though the strings are equal, we don't consider these keys as being so,
    // because we are not responsible for encoding the string to bytes and back.
    assertNotSame(skey, sbkey);
  }
}
