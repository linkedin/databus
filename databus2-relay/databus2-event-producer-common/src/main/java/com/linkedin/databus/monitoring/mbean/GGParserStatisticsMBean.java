package com.linkedin.databus.monitoring.mbean;
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


public interface GGParserStatisticsMBean
{
  /** Return the total number of transactions with at least one event from the subscribed sources */
  int getNumTransactionsWithEvents();
  /** Return the total number of transactions with zero events. */
  int getNumTransactionsWithoutEvents();
  /** Returns the total number of transactions, both with and without events. */
  int getNumTransactionsTotal();
  /** Returns the total number of files parsed*/
  int getNumFilesParsed();
  /** Returns the total number of files added*/
  int getNumFilesAdded();
  /** average file size */
  long getAvgFileSize();
  /** Returns the average size (in bytes) of a transaction ??? */
  long getAvgTransactionSize();
  /** Returns the average time (in nanoseconds) to read/parse transaction */
  long getAvgParseTransactionTimeNs();
  /** Elapsed time in milliseconds since the last read transaction. */
  long getTimeSinceLastTransactionMs();
  /** Return the total number of events processed */
  int getNumTotalEvents();
  /** Return number of errors/exceptions seen while reading trail files*/
  long getNumErrors();
  /** Return time since last trail file read ms */
  long getTimeSinceLastAccessMs();
  /** return number of files between currently read file and the latest one available */
  long getFilesLag();
  /** return number of ms between currently read file and the latest one available */
  long getTimeLag();
  /** return number of bytes between currently read file position and and of the latest file available */
  long getBytesLag();
  /** number of parse errors */
  long getNumParseErrors();
  /** number of times parser was restarted - usually because of a parsing error */
  public long getNumParseRestarts();
  /** total bytes parsed */
  public long getNumBytesTotalParsed();
  /** TS of most recent trail file */
  void reset();

}
