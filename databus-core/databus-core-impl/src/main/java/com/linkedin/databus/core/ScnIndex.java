package com.linkedin.databus.core;
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
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.apache.log4j.ConsoleAppender;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.log4j.PatternLayout;

import com.linkedin.databus.core.DbusEventBuffer.AllocationPolicy;
import com.linkedin.databus.core.util.BufferPosition;
import com.linkedin.databus.core.util.BufferPositionParser;
import com.linkedin.databus2.core.AssertLevel;



/**
 * An index that is used by the EventBuffer to store scn->offset mappings.
 *
 *  Main buffer is divided into regions (may contain multiple physical buffers).
 *
 *  ScnIndex is a buffer too, which is divided into blocks. One block per region of the main buffer.
 *  Each Block is of size SIZE_OF_SCN_OFFSET_RECORD and contains SCN and corresponding OFFSET
 *  (for the structure of the offset see BufferPosition.java)
 *
 *  Each block describes one region in the main buffer. Number of regions in the main buffer is:
 *  num_reigions = size_of_the_scn_index_buffer/SIZE_OF_SCN_OFFSET_RECORD
 *  Size of the region is: MainBufferSize/num_regions
 *
 *  To find an SCN - one should look in SCN buffer to find the starting offset (using binary search)
 *  in the main buffer and then scan sequentially starting from the offset.
 *  Event_Window may span over multiple regions. In this case all the blocks in scn index that correspond
 *  to these regions will have same values (SCN and OFFSET of the start of the window).
 *
 *  Next Event will start from the next available region (this is what is returned by getLargerOffset().
 *
 *  In ScnIndex:
 *    head - points to the next available block to read
 *    tail - next available block to write
 *
 *  Example:
 *    Main buffer size - 1000bytes
 *    Scn index size - 160bytes. 10 blocks.
 *    |---|----|---|----|---|-----|----|----|----|
 * scn| 4 | 5  | 5 | 5  | 6 | ....| 10 | 15 |    |
 * off| 0 | 100|100|100 |400| ....|700 |800 |    |
 *    |---|--- |---|----|---|-----|----|----|----|
 *      0   1   2    3              7    8    9
 *
 *
 *  (scn, data)
 *    |------|------|-------|-----|--------------------|-----------------|-----------|-----|
 * scn| 4    |   5  |  5    | 5   | 6 .......          |10,11,12,13,14   |           |     |
 *    |------|------|-------|-----|--------------------|-----------------|-----------|-----|
 *    0      100    200     300   400                  700               800
 *
 *    Event window with SCN 4 spans from offset 0 to 90.
 *    Event window with SCN 5 spans from offset 100 to 350.
 *    Event window with SCN 6 starts from offset 400.
 *    The blocks 1,2,3 of scn index will have the same SCN (5) and Offset(100). And block 4
 *    will have scn 6 and index 400.
 *
 *    Events with SCN 10-14 are all fit into region between 700 and 800, so SCN index only points
 *    at the first scn of this range. So if one need to find event with SCN 12, they'll need
 *    to do a serial scan from offset 700.
 * @author sdas
 *
 */
public class ScnIndex extends InternalDatabusEventsListenerAbstract
{
  public static final String MODULE = ScnIndex.class.getName();
  public static final Logger LOG = Logger.getLogger(MODULE);

  public static final int SIZE_OF_SCN_OFFSET_RECORD = 16;
  public static final String SCNINDEX_METAINFO_FILE_NAME = "scnIndexMetaFile";
  private final ByteBuffer buffer;
  private final  BufferPositionParser    positionParser;
  private volatile int head = -1;  // head = first valid position in the index
  private volatile int tail = 0;  // tail = position at which we start writing
  private final int maxElements;
  private long lastScnWritten = -1;
  private int lastWrittenPosition = -1;
  private final int blockSize;
  private final int individualBufferSize;
  private final ReentrantReadWriteLock rwLock;
  private boolean updateOnNext;
  private boolean isFirstCheck = true;
  private final AssertLevel _assertLevel;
  private final File _mmapSessionDirectory;

  public boolean isEnabled()
  {
    return _enabled;
  }

  private final boolean _enabled;

  private boolean updatedOnCurrentWindow = false; // see assertLastWrittenPosition

  /**
   * Public constructor : takes in the number of index entries that will be kept
   */
  public ScnIndex(int maxIndexSize, long totalAddressedSpace, int individualBufferSize,
                  BufferPositionParser parser, AllocationPolicy allocationPolicy,
                  boolean restoreBuffer, File mmapSessionDirectory, AssertLevel assertLevel,
                  boolean enabled) {
    _enabled = enabled;
    _assertLevel = null != assertLevel ? assertLevel : AssertLevel.NONE;
    rwLock = new ReentrantReadWriteLock();
    maxElements = maxIndexSize/SIZE_OF_SCN_OFFSET_RECORD;
    int proposedBlockSize = (int) (totalAddressedSpace / maxElements);
    if ((1L * proposedBlockSize * maxElements)< totalAddressedSpace)
    {
      proposedBlockSize++;
    }
    blockSize = proposedBlockSize;
    assert(1L * blockSize * maxElements >= totalAddressedSpace);

    positionParser = parser;
    int bufSize = maxElements * SIZE_OF_SCN_OFFSET_RECORD;


    if (isEnabled())
    {
      buffer = DbusEventBuffer.allocateByteBuffer(bufSize, DbusEvent.byteOrder, allocationPolicy,
                                           restoreBuffer, mmapSessionDirectory,
                                           new File(mmapSessionDirectory, "scnIndex"));
    }
    else
    {
      buffer = null;
    }

    _mmapSessionDirectory = mmapSessionDirectory;

    this.individualBufferSize = individualBufferSize;

    this.lastScnWritten = -1L;
    updateOnNext = false;
    if (!isEnabled())
    {
      LOG.info("ScnIndex not enblaed");
      return;
    }

    // See if we have metaInfoFile to set from
    if(allocationPolicy == AllocationPolicy.MMAPPED_MEMORY && restoreBuffer) {
      File metaFile = new File(mmapSessionDirectory, SCNINDEX_METAINFO_FILE_NAME);
      DbusEventBufferMetaInfo mi = null;

      if(metaFile.exists()) {
        mi = new DbusEventBufferMetaInfo(metaFile);
        try {
          mi.loadMetaInfo();
          if(mi.isValid())
            setAndValidateMetaState(mi);
          else
            throw new DbusEventBufferMetaInfo.DbusEventBufferMetaInfoException(mi, "metaInfoFile is not valid");
        } catch (DbusEventBufferMetaInfo.DbusEventBufferMetaInfoException e) {
          throw new RuntimeException(e);
        }
      } else {
        LOG.warn("restoreMMapedBuffer is set to true, but file " + metaFile + " does't exist");
      }
    }

    LOG.info("ScnIndex configured with: maxElements = " + maxElements);
  }

  /**
   * read some meta data from the file and set/verify state
   * @throws DbusEventBufferMetaInfo.DbusEventBufferMetaInfoException
   *
   */
  private void setAndValidateMetaState(DbusEventBufferMetaInfo mi) throws DbusEventBufferMetaInfo.DbusEventBufferMetaInfoException {

    LOG.info("loading metaInfoFile " + mi.toString());

    DbusEventBufferMetaInfo.BufferInfo bi = mi.getScnIndexBufferInfo();
    buffer.limit(bi.getLimit());
    buffer.position(bi.getPos());
    if(buffer.position() > buffer.limit() ||
        buffer.limit() > buffer.capacity() ||
        buffer.capacity() != bi.getCapacity()){
      throw new DbusEventBufferMetaInfo.DbusEventBufferMetaInfoException(mi,
          "ScnIndex buffer is not valid: pos =" + buffer.position() +
          "; limit = " + buffer.limit() + "; cap=" + buffer.capacity() +
          "; miCapacity= " + bi.getCapacity());
    }
    int scnBufferSize = mi.getInt("scnBufferSize");
    if(scnBufferSize != buffer.capacity())
      throw new DbusEventBufferMetaInfo.DbusEventBufferMetaInfoException(mi,
            "Invalid scnBufferSize in meta file:" + scnBufferSize + "(expected =" + buffer.capacity() + ")");

    this.lastScnWritten = mi.getLong("lastScnWritten");
    this.updateOnNext = mi.getBool("updateOnNext");
    this.head = mi.getInt("head");
    this.lastScnWritten = mi.getLong("lastScnWritten");
    this.tail = mi.getInt("tail");

    int miMaxElements = mi.getInt("maxElements");
    if(miMaxElements != maxElements || (maxElements * SIZE_OF_SCN_OFFSET_RECORD) != scnBufferSize) {
      throw new DbusEventBufferMetaInfo.DbusEventBufferMetaInfoException(mi,
            "maxElements in meta file didn't match maxElements:" + miMaxElements + " != " + maxElements);
    }

    this.lastWrittenPosition = mi.getInt("lastWrittenPosition");

    int miBlockSize = mi.getInt("blockSize");
    if(miBlockSize != blockSize) {
      throw new DbusEventBufferMetaInfo.DbusEventBufferMetaInfoException(mi,
                "miBlockSize in meta file didn't match blockSize:" + miBlockSize + " != " + blockSize);
    }

    int miIndividualBufferSize = mi.getInt("individualBufferSize");
    if(miIndividualBufferSize != individualBufferSize) {
      throw new DbusEventBufferMetaInfo.DbusEventBufferMetaInfoException(mi,
            "miIndividualBufferSize in meta file didn't match individualBufferSize:" + miIndividualBufferSize + " != " + individualBufferSize);
    }
    this.isFirstCheck = mi.getBool("isFirstCheck");
    this.updatedOnCurrentWindow = mi.getBool("updatedOnCurrentWindow");


    // run some more validations
    assertHead();
    assertTail();
    assertOrder();
    assertOffsets();
  }


  public void saveBufferMetaInfo() throws IOException {

    if (!isEnabled())
    {
      return;
    }
    DbusEventBufferMetaInfo mi = new DbusEventBufferMetaInfo(new File(_mmapSessionDirectory, SCNINDEX_METAINFO_FILE_NAME));
    LOG.info("about to save scnindex state into " + mi.toString());

    //save scnIndex file info
    mi.setScnIndexBufferInfo(new DbusEventBufferMetaInfo.BufferInfo(buffer.position(), buffer.limit(), buffer.capacity()));
    mi.setVal("scnBufferSize", Integer.toString(buffer.capacity()));

    mi.setVal("lastScnWritten", Long.toString(lastScnWritten));
    mi.setVal("updateOnNext",Boolean.toString(updateOnNext));
    //private final  BufferPositionParser    positionParser;
    mi.setVal("head", Integer.toString(head));
    mi.setVal("tail", Integer.toString(tail));
    mi.setVal("maxElements", Integer.toString(maxElements));
    mi.setVal("lastScnWritten", Long.toString(lastScnWritten));
    mi.setVal("lastWrittenPosition", Integer.toString(lastWrittenPosition));
    mi.setVal("blockSize", Integer.toString(blockSize));

    mi.setVal("individualBufferSize", Integer.toString(individualBufferSize));
    mi.setVal("isFirstCheck", Boolean.toString(isFirstCheck));
    mi.setVal("updatedOnCurrentWindow", Boolean.toString(updatedOnCurrentWindow));


    // AssertLevel _assertLevel - no need to save, may be changed between the calles.
    // File _mmapSessionDirectory; - no need to save, is passed as an arugment

    mi.saveAndClose();
  }
  /**
   * in case of MMaped file - flush the content
   */
  @Override
  public void close() {
    if (isEnabled())
    {
      flushMMappedBuffers();
    }
  }

  @Override
  public String toString()
  {
    StringBuilder sb = new StringBuilder(100);
    if (isEnabled())
    {
      sb.append("{\"head\": ");
      sb.append(head);
      sb.append(", \"headIdx\":");
      sb.append(head / SIZE_OF_SCN_OFFSET_RECORD);
      sb.append(", \"tail\":");
      sb.append(tail);
      sb.append(", \"tailIdx\":");
      sb.append(tail / SIZE_OF_SCN_OFFSET_RECORD);
      sb.append(", \"lastWrittenPosition\":");
      sb.append(lastWrittenPosition);
      sb.append(", \"lastScnWritten\":");
      sb.append(lastScnWritten);
      sb.append(", \"size\":");
      sb.append(size());
      sb.append(", \"maxSize\":");
      sb.append(maxElements);
      sb.append(", \"minScn\":");
      sb.append(getMinScn());
      sb.append(", \"blockSize\":");
      sb.append(blockSize);
      sb.append(" \"updateOnNext\":");
      sb.append(updateOnNext);
      sb.append(" \"assertLevel\":");
      sb.append(_assertLevel);
      sb.append("}\n");
    }
    else
    {
      sb.append("{\"enabled\" : false}");
    }
    return sb.toString();
  }

  //
  // print each block's content from the index
  // if the same offset - consolidate into one line
  // also (to save disk space and improve readability) - use simplified log format
  public void printVerboseString(Logger log, Level l)
  {
    // create a temp logger with simplified message
    //PatternLayout defaultLayout = new PatternLayout("%d{ISO8601} +%r [%t] (%p) {%c} %m%n");
    PatternLayout defaultLayout = new PatternLayout("%m%n");
    ConsoleAppender defaultAppender = new ConsoleAppender(defaultLayout);
    Logger newLog = Logger.getLogger("Simple");
    newLog.removeAllAppenders();
    newLog.addAppender(defaultAppender);
    newLog.setAdditivity(false);
    newLog.setLevel(Level.ALL);

    log.log(l, toString());
    if (!isEnabled())
    {
      log.log(l, "ScnIndex is disabled");
      return;
    }

    // consolidated printing:
    //     beginBlock-endBlock:SCN=>Offset
    // actual line is printed only when offset changes
    long currentOffset, beginBlock;
    long  endBlock;
    long currentScn = -1;
    currentOffset = -1;
    endBlock = beginBlock = 0;
    for (int position = 0; position < buffer.limit();  position += SIZE_OF_SCN_OFFSET_RECORD)
    {
      long nextOffset = getOffset(position);
      if (currentOffset < 0) currentOffset = nextOffset;
      long nextBlock = position/SIZE_OF_SCN_OFFSET_RECORD;
      long nextScn = getScn(position);
      if (currentScn < 0) currentScn = nextScn;
      if(nextOffset != currentOffset || currentScn != nextScn) {
        // new offset - print the previous line
        newLog.log(l, buildBlockIndexString(beginBlock, endBlock, currentScn, currentOffset));

        currentOffset = nextOffset;
        beginBlock = nextBlock;
        currentScn = nextScn;
      }

      endBlock = nextBlock;

      //log.log(p, i/SIZE_OF_SCN_OFFSET_RECORD + ":" + nextScn + "->" + nextOffset);
    }
    // final line
    newLog.log(l, buildBlockIndexString(beginBlock, endBlock, currentScn, currentOffset));
  }

  private String buildBlockIndexString(long beginBlock, long endBlock, long scn, long offset) {
    String block = "" + endBlock;
    if(beginBlock != endBlock)
      block = "[" + beginBlock + "-" + endBlock + "]";

    return block + ":" + scn + "->"+offset + " " + positionParser.toString(offset);
  }

  /**
   * Returns the minimum scn currently stored by the index
   */
  public long getMinScn() {
    if (!isEnabled())
    {
      throw new RuntimeException("ScnIndex not enabled");
    }
    try
    {
      acquireReadLock();
      if (empty())
      {
        return -1;
      }
      else
      {
        long minScn = getScn(head);
        return minScn;
      }
    }
    finally
    {
      releaseReadLock();
    }
  }


  private void acquireReadLock() {
    rwLock.readLock().lock();
  }

  private void releaseReadLock() {
    rwLock.readLock().unlock();
  }

  void acquireWriteLock() {
    rwLock.writeLock().lock();
  }

  void releaseWriteLock() {
    rwLock.writeLock().unlock();
  }

  /**
   * Moves the lastWritten position
   * @param blockNumber     the number of the SCNIndex element to update
   * @param scn             the new SCN of the above element
   * @param offset          the new offset of the above element
   */
  private void setScnOffset(int blockNumber, long scn, long offset)
  {
	  boolean isDebug = LOG.isDebugEnabled();
	  assertEquals("SCN Index Tail Check", buffer.position(),tail, AssertLevel.QUICK);

	  int startPos = tail;

	  updatedOnCurrentWindow = false;

	  // if new window in the same block we just wrote, skip :
	  // In the case of multiple window boundaries insied a single block : SCNIndex always points to the first window in that block
	  if (lastWrittenPosition == blockNumber*SIZE_OF_SCN_OFFSET_RECORD)
		  return;

	  try
	  {
		  acquireWriteLock();

		  int currentWritePosition = tail;
		  boolean overwritingHead = (currentWritePosition == head);

		  if (lastWrittenPosition >= 0 )
		  {
			  long lastWrittenScn = this.getScn(lastWrittenPosition);
			  long lastWrittenOffset = this.getOffset(lastWrittenPosition);

			  // Setup the blocks between tail and the passed block number.
			  while ((!overwritingHead) && (currentWritePosition != blockNumber*SIZE_OF_SCN_OFFSET_RECORD))
			  {
				  if ( currentWritePosition == head )
				  {
					  overwritingHead = true;
					  break;
				  }

				  if (isDebug)
					  LOG.trace("ScnIndex:Extend:" + "[" + buffer.position() + "]" +
							  	lastWrittenScn + "->" + lastWrittenOffset);

				  buffer.putLong(lastWrittenScn);
				  buffer.putLong(lastWrittenOffset);

				  lastWrittenPosition = currentWritePosition;

				  if (buffer.position() == buffer.limit())
				  {
					  buffer.position(0);
				  }

				  currentWritePosition = buffer.position();
			  }

			  if (currentWritePosition == head)
				  overwritingHead = true;
		  } else {
			  overwritingHead = false;
		  }

		  //Update the corresponding index entry only when we are not overwriting HEAD.
		  if ( !overwritingHead )
		  {
			  if (isDebug)
				  LOG.debug("ScnIndex:Write:"+"["+buffer.position()+"]"+scn+"->"+offset);

			  buffer.putLong(scn);
			  buffer.putLong(offset);
			  lastScnWritten = scn;
			  lastWrittenPosition = currentWritePosition;
			  updatedOnCurrentWindow = true;

			  if (buffer.position() == buffer.limit())
			  {
				  buffer.position(0);
			  }

			  currentWritePosition = buffer.position();
		  }

		  if (head < 0)
		  {
			  // Buffer was empty, we should have written starting from startPosition
			  head = startPos;
		  }

		  // Set tail to be ready for the next write
		  tail = buffer.position();

		  if ( isDebug)
		  {
			  LOG.debug("Setting SCNIndex tail to :" + tail + " after writing EVB offset :"
					  + positionParser.toString(offset) + " for SCN : " + scn
					  + " till block number :" + blockNumber);
		  }

          if (_assertLevel.quickEnabled())
          {
            assertHead();
            assertTail();

            if (_assertLevel.mediumEnabled())
            {
              assertOrder();
              assertOffsets();
            }
          }
	  } finally {
		  releaseWriteLock();
	  }
  }

  int getBlockNumber(long offset)
  {
    if (!isEnabled())
    {
      throw new RuntimeException("ScnIndex not enabled");
    }
    long bufferIndex = positionParser.bufferIndex(offset);
    long buf_offset = positionParser.bufferOffset(offset);
    long blockNumber = (bufferIndex * this.individualBufferSize + buf_offset) / blockSize;
    return (int) blockNumber;
  }

  void flushMMappedBuffers()
  {
    if (!isEnabled())
    {
      return;
    }
    if (buffer instanceof MappedByteBuffer) ((MappedByteBuffer)buffer).force();
  }

  /*
   * ScnIndexEntry
   *
   * Wraps the contents of each record (scn, offset)
   * Primarily used for passing this info to the caller
   */
  public class ScnIndexEntry
  {
	 private long _scn;
	 private long _offset;

	 public long getScn() {
		return _scn;
	 }

	 public void setScn(long scn) {
		this._scn = scn;
	 }

	 public long getOffset() {
		return _offset;
	 }

	 public void setOffset(long offset) {
		this._offset = offset;
	 }

	public ScnIndexEntry(long scn, long offset) {
		super();
		this._scn = scn;
		this._offset = offset;
	}
  }

  /*
   *
   */
  public ScnIndexEntry getClosestOffset(long searchScn) throws OffsetNotFoundException {

    if (!isEnabled())
    {
      throw new RuntimeException("ScnIndex not enabled");
    }
    if (empty()) {
      LOG.info("ScnIndex is empty");
      throw new OffsetNotFoundException();
    }
    // binary search
    int left = head;
    int right = tail;
    int index;
    index = midPoint(left, right, buffer.limit());

    long currScn = getScn(index);
    boolean found = false;
    while (!found)
    {
      if (isClosestScn(currScn, searchScn, index)) {
        int lessIndex = decrement(index, buffer.limit());
        long lessScn = getScn(lessIndex);
        while ((index!=head) && (currScn == lessScn))
        {
          index = lessIndex;
          currScn = lessScn;
          lessIndex = decrement(index, buffer.limit());
          lessScn = getScn(lessIndex);
        }
        found = true;
        return  new ScnIndexEntry(currScn,getOffset(index));
      }
      else {
        if (currScn > searchScn) {
          if ((index == right) || ((left + SIZE_OF_SCN_OFFSET_RECORD)%buffer.limit() == right)){
            LOG.error("Case 1 : currScn > searchScn and index == right" +
            		  "index = " + index +
                      " right = " + right +
                      " left = " + left +
                      " buffer.limit = " + buffer.limit() +
                      " searchScn = " + searchScn +
                      " currScn = " + currScn);
            printVerboseString(LOG, Level.ERROR);
            throw new OffsetNotFoundException();
          }
          right = index;
        }
        else {
          if (index == left) {
            LOG.error("Case 2 : currScn <= searchScn and index == left" +
            		  "index = " + index +
                      " right = " + right +
                      " left = " + left +
                      " buffer.limit = " + buffer.limit() +
                      " searchScn = " + searchScn +
                      " currScn = " + currScn);
            printVerboseString(LOG, Level.ERROR);
            throw new OffsetNotFoundException();
          }
          left = index;
        }
        int prevIndex = index;
        index = midPoint(left, right, buffer.limit());
        if (prevIndex == index) {
          LOG.error("Case 3 : currScn > searchScn and prevIndex == index" +
        		    "index = " + index +
                    " prevIndex = " + prevIndex +
                    " right = " + right +
                    " left = " + left +
                    " buffer.limit = " + buffer.limit() +
                    " searchScn = " + searchScn +
                    " currScn = " + currScn);
          printVerboseString(LOG, Level.ERROR);
          throw new OffsetNotFoundException();
        }
        currScn = getScn(index);

      }
    }

    return  new ScnIndexEntry(currScn,getOffset(index));
  }


  private boolean empty() {
    return (numElements(head, tail, buffer.limit()) == 0);
  }


  /**
   * Returns the scn of an index entry
   * @param entryOffset     the physical offset of the entry in the index buffer
   */
  private long getScn(int entryOffset) {
    return buffer.getLong(entryOffset);
  }

  /**
   * Returns the offset of an index entry
   * @param entryOffset     the physical offset of the entry in the index buffer
   */
  private long getOffset(int entryOffset) {
    return buffer.getLong(entryOffset + 8);
  }

  private int entryIndexOfs(int entryIndex)
  {
    return (head + entryIndex * SIZE_OF_SCN_OFFSET_RECORD) % (SIZE_OF_SCN_OFFSET_RECORD * maxElements);
  }

  /**
   * Returns the scn of an index entry
   * @param entryIndex      the index of the entry relative to the head
   * @return the scn or -1 if not found
   */
  private long getEntryScn(int entryIndex)
  {
    if (-1 == head) return -1;
    return getScn(entryIndexOfs(entryIndex));
  }

  /**
   * Returns the offset of an index entry
   * @param entryIndex      the index of the entry relative to the head
   * @return the scn or -1 if not found
   */
  private long getEntryOffset(int entryIndex)
  {
    if (-1 == head) return -1;
    return getOffset(entryIndexOfs(entryIndex));
  }

  private boolean isClosestScn(long indexScn, long searchScn, int index) {
    try
    {
      acquireReadLock();
      if (indexScn == searchScn) {
        return true;
      }
      if (indexScn < searchScn) {
        if ((index + SIZE_OF_SCN_OFFSET_RECORD)%buffer.limit() == tail) {
          return true;
        }
        else
        {
          return (getScn((index+SIZE_OF_SCN_OFFSET_RECORD)%buffer.limit()) > searchScn);
        }
      }
      return false;
    }
    finally
    {
      releaseReadLock();
    }
  }

  private int size()
  {
    return ScnIndex.numElements(head, tail, maxElements * SIZE_OF_SCN_OFFSET_RECORD);
  }

  private static int numElements(int left, int right, int end) {
    if (left < 0) { return 0; }
    if (left == right) { return end/SIZE_OF_SCN_OFFSET_RECORD; }
    if ( left < right ) {
      return ((right - left)/SIZE_OF_SCN_OFFSET_RECORD);
    }
    else {
      return (end - left + right)/SIZE_OF_SCN_OFFSET_RECORD;
    }
  }

  @SuppressWarnings("unused")
  private boolean full() {
    return (numElements(head, tail, buffer.limit()) == maxElements);
  }

  private static int midPoint(int left, int right, int end) {
    int size = numElements(left, right, end);
    return (left + (size/2)*SIZE_OF_SCN_OFFSET_RECORD) % end;
  }

  private static int decrement(int index, int end) {
    if (index >= SIZE_OF_SCN_OFFSET_RECORD)
    {
      return (index - SIZE_OF_SCN_OFFSET_RECORD);
    }
    else
    {
      return (end - SIZE_OF_SCN_OFFSET_RECORD);
    }
  }


  public void clear() {
    if(!isEnabled())
    {
      return;
    }
    buffer.rewind();
    head = -1;
    tail = 0;
    this.lastScnWritten = -1L;
    lastWrittenPosition = head;
    updateOnNext = false;

  }

  @Override
  public void onEvent(DataChangeEvent event, long offset, int size)
  {
    if(!isEnabled())
    {
      return;
    }
    boolean shouldUpdate = shouldUpdate(event);
    if (LOG.isDebugEnabled())
      LOG.debug("ScnIndex:onEvent:offset" + offset + " Event:"+event.sequence()+";shouldUpdate="+shouldUpdate+";eop="+event.isEndOfPeriodMarker());


    if (shouldUpdate)
    {
      // event window scn should be monotonically increasing
      if (event.sequence()<lastScnWritten)
      {
        throw new RuntimeException("Event Sequence " + event.sequence() + " < lastScnWritten " + lastScnWritten);
      }
      int blockNumber = getBlockNumber(offset);
      setScnOffset(blockNumber, event.sequence(), offset);
    }
  }

  private boolean shouldUpdate(DataChangeEvent event) {
    boolean returnVal =  (updateOnNext && !event.isControlMessage());
    updateOnNext = (event.isEndOfPeriodMarker());
    return returnVal;
  }

  public boolean isEmpty()
  {
    if (!isEnabled())
    {
      throw new RuntimeException("ScnIndex not enabled");
    }
      return head==-1;
  }

  protected void setUpdateOnNext(boolean val)
  {
    if (!isEnabled())
    {
      return;
    }
    updateOnNext = val;
  }

  protected boolean getUpdateOnNext()
  {
    if (!isEnabled())
    {
      throw new RuntimeException("ScnIndex not enabled");
    }
    return updateOnNext;
  }

  /**
   * Return a valid offset (which represents a valid event window start point) which is larger than the offset passed in
   * If there are no valid offsets then return -1
   * @param offset
   */
  public long getLargerOffset(long offset) {
    if (!isEnabled())
    {
      throw new RuntimeException("ScnIndex not enabled");
    }
	/*
	 * TODO: Longer-Term fix : DDSDBUS-386 : Implement logic to enable setting up the head to a window whose block number
	 * is same as the current Head position's block number
	 */
    // Note: not acquiring read lock because this is called by the writer
    boolean trace = false;
    int blockNumber = getBlockNumber(offset);
    long currentOffset = getOffset(blockNumber*SIZE_OF_SCN_OFFSET_RECORD);
    // the retrieved offset could be < , == or > than the passed in offset .. haha !
    int maxIterations = numElements(head, tail, buffer.limit());

    if (trace)
    {
      LOG.info("offset = " + offset + ";blockNumber = " + blockNumber + ";currentOffset = " + currentOffset + ";currentScn = " + getScn(blockNumber*SIZE_OF_SCN_OFFSET_RECORD));
      LOG.info("maxIterations = "  + maxIterations);
    }
    long prevOffset = currentOffset;
    while ((prevOffset == currentOffset) && (maxIterations > 0))
    {
      blockNumber++;
      if (blockNumber >= maxElements)
      {
        blockNumber =0;
      }
      prevOffset = currentOffset;
      currentOffset = getOffset(blockNumber*SIZE_OF_SCN_OFFSET_RECORD);
      --maxIterations;
    }



    // either currentOffset is > offset or it is wrapped around or we couldn't find a valid candidate
    if (maxIterations ==0)
    {
      return -1;
    }

    if (trace)
    {
      LOG.info("Returning currentOffset = " + currentOffset + " prevOffset = " + prevOffset + " offset = "+ offset);
    }
    return currentOffset;
  }

  void moveHead(long offset)
  {
    if (!isEnabled())
    {
      return;
    }
    moveHead(offset, -1);
  }

  /**
   * This method is to get around a problem with the ScnIndex when an Iterator is trying to
   * remove events at the head of a window. If the window is pointed by entries in the ScnIndex
   * after the removal, these entries will become invalid.
   *
   * THIS IS A HACK! After this look ups in the index will return partial results for this window.
   * Luckily currently the iterator removal is used only by the client which does not use the
   * ScnIndex.
   *
   * TODO: This has to be fixed
   *
   * @param newHeadOffset      the new event buffer head
   */
  void moveHead(long newHeadOffset, long newHeadScn)
  {
    if (!isEnabled())
    {
      return;
    }
    boolean debugEnabled = LOG.isDebugEnabled();

    int proposedHead;
    int blockNumber = -1;
    if (newHeadOffset < 0)
    {
      if (head >= 0) LOG.warn("track(ScnIndex.head): resetting head to -1: " + newHeadOffset);
      proposedHead = -1;
    }
    else
    {
      blockNumber = getBlockNumber(newHeadOffset);
      proposedHead = blockNumber * SIZE_OF_SCN_OFFSET_RECORD;
    }

    acquireWriteLock();
    try
    {
      //we have to be careful about the following situations:
      // |---H---T---PH----| or |---T--PH----H---|
      // The above can happen if we are on the client side, and start removing events from a window
      // before we've seen the end of the window. In that case, the tail has not been updated yet
      // and the proposed head can overshoot the tail.
      boolean moveTail = newHeadScn >= 0 &&
          (head < tail && tail <= proposedHead) || (proposedHead >= tail && proposedHead < head);

      if (moveTail && blockNumber >= 0)
      {
        long oldHeadScn = head >= 0 ? getScn(head) : -1;
        long oldHeadOfs = head >= 0 ? getOffset(head) : -1;

        if (0 == proposedHead && head > 0)
        {
          LOG.info("in:" + newHeadScn);
        }

        //Update the tail to match the new head position
        if (debugEnabled)
        {
          LOG.debug("adjusting tail to match net head position; before: proposedHead=" + proposedHead +
                   " newOfs=" + newHeadOffset + " newScn=" + newHeadScn + " oldScn=" + oldHeadScn
                   + " oldOfs=" + oldHeadOfs);
          LOG.debug(this);
        }
        setScnOffset(blockNumber, newHeadScn, oldHeadScn == newHeadScn ? oldHeadOfs : newHeadOffset);
        if (debugEnabled)
        {
          LOG.debug("adjusting tail to match net head position; after " + this);
        }
      }


      head = proposedHead;
      if (LOG.isDebugEnabled())
      {
        LOG.debug("After move head: " + this);
      }

      if (head >= 0)
      {
        long oldHeadOfs = getOffset(head);
        if (oldHeadOfs != newHeadOffset)
        {
          //we need to update the Offset for all adjacent entries with the same offset as the head
          int i = head;
          int n = size();
          long curOfs = oldHeadOfs;
          //LOG.info("i=" + i + " n=" + n + " curOfs=" + curOfs + " headOfs=" + headOfs);
          //this.printVerboseString(LOG, Level.ERROR);
          do
          {
            buffer.putLong(i + 8, newHeadOffset);
            i += SIZE_OF_SCN_OFFSET_RECORD;
            if (buffer.limit() - i < SIZE_OF_SCN_OFFSET_RECORD) i = 0;
            n--;

            if (n >0 ) curOfs = getOffset(i);
          }
          while (i != tail && n > 0 && curOfs == oldHeadOfs);
          //LOG.info("i=" + i + " n=" + n + " curOfs=" + curOfs + " headOfs=" + headOfs);
          //this.printVerboseString(LOG, Level.ERROR);
        }
      }
      else
      {
        tail = 0;
      }

      if (_assertLevel.quickEnabled())
      {
        assertHead();
        assertTail();
      }

    }
    finally
    {
      releaseWriteLock();
    }
  }

  /*
   * Non-Junit Utility method for assertions.
   */
  private void assertEquals(String message, long exp, long actual, AssertLevel level)
  {
	  if (_assertLevel.getIntValue() < level.getIntValue())
		  return;

	  if (exp != actual)
	  {
		  LOG.fatal("FAIL :" + message + ", Expected :" + exp + ", Actual :" + actual);
		  throw new RuntimeException("FAIL :" + message + ", Expected :" + exp + ", Actual :" + actual);
	  }
  }

 /*
  * Asserts that EVB Head and SCNINdex Head is consistent
  */
  public void assertHeadPosition(long evbHead)
  {
    if (!isEnabled())
    {
      return;
    }
    long expHead = -1;
    if ( evbHead > 0 )
    {
      int blockNumber = getBlockNumber(evbHead);
      expHead = blockNumber * SIZE_OF_SCN_OFFSET_RECORD;

      if (expHead != head)
      {
        String msg = "SCN Index Head is (" + head + ") Expected Head was (" + expHead +") Event Buffer Head is :" + evbHead;
        LOG.fatal(msg);
        LOG.fatal("SCN Index is (assertHeadPosition):");
        printVerboseString(LOG, org.apache.log4j.Level.FATAL);
        throw new RuntimeException(msg.toString());
      }
    }
  }

  /*
   * Asserts that EVB eventStartIndex and SCNINdex lastWrittenPosition is consistent
   * This is a consistency check called in endEvents after the SCNIndex is updated.
   * eventStartIndex is the EVB offset to the beginning of the current window (whose endEvents() is being processed)
   * lastWrittenPosition refers to the last SCNIndex block we updated.
   *
   *   Except for the following cases, both of these values should be equal
   *   a) First Event Window written to the EVB
   *   b) when overWritingHead was detected while trying to update the SCNIndex for the current window.
   */
   public void assertLastWrittenPos(BufferPosition evbStartPos)
   {
     if (!isEnabled())
     {
       return;
     }
     long expTail = -1;
     long evbStartLoc = evbStartPos.getRealPosition();

     if ( (evbStartPos.getPosition() > 0) && (!isFirstCheck) && updatedOnCurrentWindow)
     {
       int blockNumber = getBlockNumber(evbStartLoc);
       expTail = blockNumber * SIZE_OF_SCN_OFFSET_RECORD;

       if (expTail != lastWrittenPosition)
       {
         StringBuilder msg = new StringBuilder();
         msg.append("SCN Index LWP is (" + lastWrittenPosition  + ") Expected LWP was (" + expTail +") Event Buffer Start Index is :" + evbStartPos);
         msg.append("SCN Index is(assertLastWrittenPos) :" + toString());
         msg.append("SCN at LWP: " + getScn(lastWrittenPosition) + ", Offset :" + positionParser.toString(getOffset(lastWrittenPosition)));
         LOG.fatal(msg);
         throw new RuntimeException(msg.toString());
       }
     }
     isFirstCheck  = false;
   }

   private void smartVerbosePrint()
   {
     if (LOG.isDebugEnabled() || maxElements < 10000)
     {
       printVerboseString(LOG, Level.ERROR);
     }
   }

   private void assertTail()
   {
     if (empty())
     {
       if (0 != tail) throw new AssertionError("invalid empty tail: " + tail + "; state: " + this);
     }
     else if (0 > tail || maxElements * SIZE_OF_SCN_OFFSET_RECORD <= tail)
     {
       throw new AssertionError("invalid tail: " + tail + "; state: " + this);
     }
     else if (0 != tail % SIZE_OF_SCN_OFFSET_RECORD)
     {
       throw new AssertionError("tail not aligned: " + tail + "; state: " + this);
     }
   }

   private void assertHead()
   {
     if (empty())
     {
       if (head != -1) throw new AssertionError("invalid empty head: " + head+ "; state: " + this);
     }
     else
     {
       if (0 != head % SIZE_OF_SCN_OFFSET_RECORD)
       {
         throw new AssertionError("head not aligned: " + head + "; state: " + this);
       }

       if (0 > head || maxElements * SIZE_OF_SCN_OFFSET_RECORD <= head)
       {
         throw new AssertionError("invalid head: " + head + "; state: " + this);
       }
     }
   }

   private void assertOrder()
   {
     int sz = size();
     if (0 == sz) return;

     for (int i = 0; i < sz - 1; ++i)
     {
       long scn1 = getEntryScn(i);
       long scn2 = getEntryScn(i + 1);
       long ofs1 = getEntryOffset(i);
       long ofs2 = getEntryOffset(i + 1);

       if (scn1 > scn2)
       {
         smartVerbosePrint();
         throw new AssertionError("scn[" + i + "] = " + scn1 + " > " + scn2 + " = scn[" + (i + 1) +
                                  "]; \nstate:" + this);
       }
       else if (scn1 == scn2)
       {
         if (ofs1 != ofs2)
         {
           smartVerbosePrint();
           throw new AssertionError("scn[" + i + "] = " + scn1 + " == " + scn2 + " = scn[" + (i + 1) +
                                    "] but ofs[" + i + "] = " + ofs1 + "!= " + ofs2 + " = ofs[" + (i + 1) +
                                    "]; \nstate:" + this);
         }
       }
       else if (ofs1 >= ofs2)
       {
         smartVerbosePrint();
         throw new AssertionError("ofs[" + i + "] = " + ofs1 + " >= " + ofs2 + " = ofs[" + (i + 1) +
                                  "] but scn[" + i + "] = " + scn1 + " < " + scn2 + " = scn[" + (i + 1) +
                                  "]; \nstate:" + this);
       }
     }
   }

   private void assertOffsets()
   {
     int sz = size();
     if (0 == sz) return;

     long minGen = Long.MAX_VALUE;
     long maxGen = Long.MIN_VALUE;
     int minGenIdx = -1;
     int maxGenIdx = -1;
     long minOfs = -1;
     long maxOfs = 2;

     int lastNewOfs = -1;
     int prevOfs = -1;
     for (int i = 0; i < sz; ++i)
     {
       int indexOfs = entryIndexOfs(i); //physical offset in the index of the i-th entry

       long ofs1 = getOffset(indexOfs); //offset in the event buffer for the i-th entry
       long ofs_1 = prevOfs != -1 ? getOffset(prevOfs) : -1; //offset in the event buffer for the (i-1)-th entry

       long gen1 = positionParser.bufferGenId(ofs1);

       long block1 = getBlockNumber(ofs1);
       int expBlock = -1;

       if (i == 0 || ofs1 != ofs_1) expBlock = indexOfs / SIZE_OF_SCN_OFFSET_RECORD;
       else expBlock = lastNewOfs / SIZE_OF_SCN_OFFSET_RECORD;

       if (gen1 < minGen)
       {
         minGen = gen1;
         minGenIdx = i;
         minOfs = ofs1;
       }
       if (gen1 > maxGen)
       {
         maxGen = gen1;
         maxGenIdx = i;
         maxOfs = ofs1;
       }

       if (expBlock != block1  )
       {
         smartVerbosePrint();
         throw new AssertionError("block(offset[" + i + "]=" + ofs1 + ")=" + block1 + " != " +
                                  expBlock + "; state:" + this);
       }

       if (ofs1 != ofs_1) lastNewOfs = indexOfs;
       prevOfs = indexOfs;
     }

     if (maxGen - minGen > 1)
     {
       smartVerbosePrint();
       throw new AssertionError("genId (ofs[" + minGenIdx + "]) + 1 = " +
                                (minGen + 1) + " < " + maxGen + " = genId(ofs[ " + maxGenIdx +
                                "]); minOfs=" + minOfs + "->" + positionParser.toString(minOfs) +
                                "; maxOfs=" + maxOfs + "->" + positionParser.toString(maxOfs) +
                                "; \nstate:" + this);
     }

   }

   /**
    * package private to allow helper classes to inspect internal details
    */
   int getHead() {
     if (!isEnabled())
     {
       throw new RuntimeException("ScnIndex not enabled");
     }
	   return head;
   }

   /**
    * package private to allow helper classes to inspect internal details
    */
   int getTail() {
     if (!isEnabled())
     {
       throw new RuntimeException("ScnIndex not enabled");
     }
     return tail;
   }

  /** The position parser used byt the index (for testing purposes) */
  BufferPositionParser getPositionParser()
  {
    if (!isEnabled())
    {
      throw new RuntimeException("ScnIndex not enabled");
    }
    return positionParser;
  }
}
