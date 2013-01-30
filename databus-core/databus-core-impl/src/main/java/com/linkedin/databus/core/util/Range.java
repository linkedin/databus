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


/**
 * A class representing a range of offsets from startOffset to endOffset
 * All elements from startOffset until endOffset-1 are included in the range.
 * Two ranges are comparable based on their startOffsets only.
 *
 * @author sdas
 *
 */
public class Range implements Comparable<Range> {
	public Range(long startOffset, long endOffset) {
		start = startOffset;
		end = endOffset;
	}
	public long start;
	public long end;

	public Range()
	{
	}

	public boolean contains(long someOffset)
	{
		return contains(start, end, someOffset);
	}


	/*
	 * Method to check if the writer is going to overwrite the readerPosition
	 *
	 * Writer is expected to be ahead of the reader.
	 * Assumes the input contains the GenId (Look at the edge-case below)
	 *
	 */
	public static boolean containsReaderPosition(long writerStart,
												long writerEnd,
												long readerPosition,
												BufferPositionParser parser)
	{
	  if (readerPosition < 0 ) return false; //empty
	  //just make the reader look the same generation as the writer
	  if (parser.bufferGenId(readerPosition) < parser.bufferGenId(writerStart))
	  {
	    long fakeReaderPos = parser.setGenId(readerPosition, parser.bufferGenId(writerStart));
	    return writerStart <= fakeReaderPos && fakeReaderPos < writerEnd;
	  }
	  else
	  {
	    return writerStart <= readerPosition && readerPosition < writerEnd;
	  }
	}



	public static boolean containsIgnoreGenId(long start,
			                                  long end,
			                                  long offset,
			                                  BufferPositionParser parser)
	{

		return contains(parser.address(start),
						parser.address(end),
						parser.address(offset));
	}


	public static boolean contains(long start, long end, long someOffset)
	{
		// Range is [start, end) , so the end position is not going to be written to.
		if (someOffset < 0)
		{
			return false;
		}

		if (start < end)  // |------ start xxxxxxxxxxx end -------|
		{
			if ((start< someOffset) && (end <= someOffset))
			{
				return false;
			}
			if ((start > someOffset) && (end > someOffset))
			{
				return false;
			}
			return true;
		}

		if (start > end) // |-------end---------start----------|
		{
			if ((start > someOffset) && (end <= someOffset))
			{
				return false;
			}
			return true;
		}

		return false;
	}


	public boolean intersects(Range intersectedRange) {


		if (contains(intersectedRange.start) || intersectedRange.contains(start))
		{
			return true;
		}

		return false;
	}

	@Override
	public int compareTo(Range comparedRange) {
		if (start != comparedRange.start)
		{
            // Since GenIds/Index are more likely in the MSBs of long, type reduction to int will not work
		    // when comparing entries differing in indexes/genIds
			//return (int) (start - comparedRange.start);
			return start > comparedRange.start ? 1 : -1;
		}
		else
		{
		    // Since GenIds/Index are more likely in the MSBs of long, type reduction to int will not work
            // when comparing entries differing in indexes/genIds
			//return (int) (end - comparedRange.end);
			return end  > comparedRange.end ? 1 : ((end == comparedRange.end) ? 0 : -1);

		}

	}


	public String toString(BufferPositionParser parser) {
		StringBuilder sb = new StringBuilder();
		sb.append("{start:");
		sb.append(parser.toString(start));
		sb.append(" - end:");
		sb.append(parser.toString(end));
		sb.append("}");
		return sb.toString();
	}


	@Override
	public String toString() {
		return "Range [start=" + start + ", end=" + end + "]";
	}

	public long getStart() {
		return start;
	}

	public void setStart(long start) {
		this.start = start;
	}

	public long getEnd() {
		return end;
	}

	public void setEnd(long end) {
		this.end = end;
	}

	@Override
	public boolean equals(Object obj)
	{
		if ( ! (obj instanceof Range))
			return false;

		Range r = (Range)obj;

		if ((r.getStart() == start) && (r.getEnd() == end))
			return true;

		return false;
	}

	@Override
	public int hashCode()
	{
		return (int)start;
	}
}
