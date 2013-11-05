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
package com.linkedin.databus.core.util;

import java.nio.ByteBuffer;

import junit.framework.Assert;

import org.testng.annotations.Test;

/**
 * Unit tests for {@link Fnv1aHashImpl}
 */
public class TestFnv1aHashImpl
{
  @Test
  /** Test the basic hashing operations -- adding a byte*/
  public void testBasic()
  {
    final long PRIME = 16777619;
    final long BASIS = 2166136261L;

    final long seed = Fnv1aHashImpl.init32();
    Assert.assertEquals(BASIS, seed);

    Assert.assertEquals((BASIS ^ 0) * PRIME, Fnv1aHashImpl.addByte32(seed, (byte)0));
    Assert.assertEquals((BASIS ^ 123) * PRIME, Fnv1aHashImpl.addByte32(seed, (byte)123));
    Assert.assertEquals((BASIS ^ 0x9C) * PRIME, Fnv1aHashImpl.addByte32(seed, (byte)-100));
    Assert.assertTrue(Fnv1aHashImpl.addByte32(seed, (byte)-101) != Fnv1aHashImpl.addByte32(seed, (byte)-100));
    Assert.assertEquals((((BASIS ^ 0x9C) * PRIME) ^ 0x50) * PRIME,
                        Fnv1aHashImpl.addByte32(Fnv1aHashImpl.addByte32(seed, (byte)-100), (byte)80));
  }

  @Test
  /** Tests various addXYZ32 methods*/
  public void testAdd32()
  {
    final long seed = Fnv1aHashImpl.init32();
    Assert.assertEquals(Fnv1aHashImpl.addByte32(Fnv1aHashImpl.addByte32(seed, (byte)0xFE), (byte)0xDC),
                        Fnv1aHashImpl.addShort32(seed, (short)0xFEDC));

    Assert.assertEquals(Fnv1aHashImpl.addBytes32(seed, new byte[]{(byte)0x12, (byte)0x34, (byte)0x56, (byte)0x78}, 0, 4),
                        Fnv1aHashImpl.addInt32(seed, 0x12345678));

    Assert.assertEquals(Fnv1aHashImpl.addBytes32(seed, new byte[]{(byte)0xDE, (byte)0xAD, (byte)0xCA, (byte)0xFE,
                                                                  (byte)0xBE, (byte)0xEF, (byte)0xBA, (byte)0xBE}, 0, 8),
                        Fnv1aHashImpl.addLong32(seed, 0xDEADCAFEBEEFBABEL));

    Assert.assertEquals(Fnv1aHashImpl.addBytes32(seed, new byte[]{(byte)0xDE, (byte)0xAD, (byte)0xCA, (byte)0xFE,
                                                                  (byte)0xBE, (byte)0xEF, (byte)0xBA, (byte)0xBE}, 1, 4),
                        Fnv1aHashImpl.addInt32(seed, 0xADCAFEBE));

    final byte[] a = new byte[]{(byte)0x12, (byte)0x34, (byte)0x56, (byte)0x78, (byte)0xBE, (byte)0xEF, (byte)0xBA,
                                (byte)0xBE};
    final ByteBuffer bb = ByteBuffer.wrap(a.clone());

    Assert.assertEquals(Fnv1aHashImpl.addBytes32(seed, a, 2, 5),
                        Fnv1aHashImpl.addByteBuffer32(seed, bb, 2, 5));

    final ByteBuffer bb2 = ByteBuffer.allocateDirect(8);
    for (byte b: a) bb2.put(b);

    Assert.assertEquals(Fnv1aHashImpl.addBytes32(seed, a),
                        Fnv1aHashImpl.addByteBuffer32(seed, bb2));

    Assert.assertEquals(Fnv1aHashImpl.addBytes32(seed, new byte[]{(byte)116, //t
                                                                  (byte)101, //e
                                                                  (byte)115, //s
                                                                  (byte)116, //t
                                                                  (byte)83,  //S
                                                                  (byte)116, //t
                                                                  (byte)114, //r
                                                                  (byte)105, //i
                                                                  (byte)110, //n
                                                                  (byte)103  //g
                                                                  }),
                       Fnv1aHashImpl.addString32(seed, "testString"));
  }

}
