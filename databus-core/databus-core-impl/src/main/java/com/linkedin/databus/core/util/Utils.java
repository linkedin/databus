package com.linkedin.databus.core.util;
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
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.io.UnsupportedEncodingException;
import java.lang.management.ManagementFactory;
import java.math.BigInteger;
import java.net.DatagramSocket;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.HashSet;
import java.util.Set;

public class Utils
{
  /**
   * The set of ports assigned by {@link #getAvailablePort(int)}. This is to avoid race conditions
   * by different tests using the same port.
   */
  private static final Set<Integer> _assignedPorts = new HashSet<Integer>();

	public static long getUnsignedInt(ByteBuffer buffer, int index) {
		return (buffer.getInt(index) & 0xffffffffL);
	}

	public static void putUnsignedInt(ByteBuffer buffer, int index, long value) {
		buffer.putInt(index, (int) (value & 0xffffffffL));
	}


	/**
	 * Translate the given buffer into a string
	 * @param buffer The buffer to translate
	 * @param encoding The encoding to use in translating bytes to characters
	 * @throws UnsupportedEncodingException
	 */
	public static String byteBufferToString(ByteBuffer buffer, String encoding) throws UnsupportedEncodingException {
		byte[] bytes = new byte[buffer.remaining()];
		buffer.get(bytes);
		return (new String(bytes, encoding)	);
	}

	public static String byteBufferToString(ByteBuffer buffer)  {
		return (new String(byteBufferToBytes(buffer)));
	}

	public static byte[] byteBufferToBytes(ByteBuffer buffer)  {
		byte[] bytes = null;
		if (buffer.hasArray())
		{
			bytes = buffer.array();
		}
		else
		{
			bytes = new byte[buffer.remaining()];
			buffer.get(bytes);
		}
		return bytes;
	}

	/**
	 * Open a channel for the given file
	 * @throws FileNotFoundException
	 */
	 public static FileChannel openChannel(File file, boolean mutable) throws FileNotFoundException  {
		 if(mutable)
			 return (new RandomAccessFile(file, "rw").getChannel());
		 else
			 return (new FileInputStream(file).getChannel());
	 }

		/*
		 * Utility method to convert an InetSocketAdress (ipAddress + port) to BigInteger
		 * Useful for ordering objects like DatabusServerCoordinates which are based on INetSocketAddresses
		 */
		public static BigInteger ipToBigInt(InetSocketAddress ipAddr)
		{
			final int BYTES_IN_INTEGER = 4;

			byte[] addrBytes = ipAddr.getAddress().getAddress();

			int port = ipAddr.getPort();


			byte[] addrPlusPortBytes = new byte[addrBytes.length + BYTES_IN_INTEGER];
			ByteBuffer destBuf = ByteBuffer.wrap(addrPlusPortBytes);
			destBuf.put(addrBytes);
			destBuf.putInt(port);

			BigInteger bigInt = new BigInteger(destBuf.array());

		    return bigInt;
		}


		/**
		 * Find the first available port after a given port
		 * @return an available port
		 **/
		public static synchronized int getAvailablePort(int startPort)
		{
			int port = startPort;

			while (_assignedPorts.contains(port) && ! portAvailable(port) &&
			      port < 0x7FFFFFFF)
			{
				port++;
			}

			if (0x7FFFFFFF == port)
			{
			  throw new RuntimeException("no available port found starting at " + startPort);
			}

			_assignedPorts.add(port);
			return port;
		}


		/**
		 * Checks to see if a specific port is available.
		 *
		 * @param port the port to check for availability
		 */
		public static boolean portAvailable(int port) {
			if (port < 1200 || port > 65000) {
				throw new IllegalArgumentException("Invalid start port: " + port);
			}

			ServerSocket ss = null;
			DatagramSocket ds = null;
			try {
				ss = new ServerSocket(port);
				ss.setReuseAddress(true);
				ds = new DatagramSocket(port);
				ds.setReuseAddress(true);
				return true;
			} catch (IOException e) {
			} finally {
				if (ds != null) {
					ds.close();
				}

				if (ss != null) {
					try {
						ss.close();
					} catch (IOException e) {
						/* should not be thrown */
					}
				}
			}

			return false;
		}

		/**
		 *
		 * @return process PID
		 */
		public static String getPid()
	  {
	    String name = ManagementFactory.getRuntimeMXBean().getName();
	    String[] values=name.split("@");
	    String pid = values[0];
	    return pid;
	  }
}
