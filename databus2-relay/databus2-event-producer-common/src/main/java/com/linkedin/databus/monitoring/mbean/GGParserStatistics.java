package com.linkedin.databus.monitoring.mbean;

import com.linkedin.databus.core.DbusConstants;


public class GGParserStatistics implements GGParserStatisticsMBean
{
  /**
   * statistical info about transaction
   */
  public static class TransactionInfo {
    public long getTransactionSize()
    {
      return _transactionSize;
    }

    public long getTransactionTimeRead()
    {
      return _transactionTimeReadNs;
    }

    public long getTransactionTimeStampNs()
    {
      return _transactionEventTSNs;
    }

    /**
     * scn of the most recent 'processes/subscribed' transaction
     * @return
     */
    public long getScn()
    {
      return _scn;
    }

    private final long _transactionSize;
    private final long _transactionTimeReadNs;
    private final long _transactionEventTSNs; // of the event in the transaction
    private final long _scn;


    /**
     *
     * @param size - size of the event
     * @param timeReadNs - The time to read the event from the trail file in Ns
     * @param tsNs The timestamp associated with the current transaction (The commit timestamp)
     * @param scn The scn associated with the transaction (If there are multiple scns(not ideal), this is the max scn)
     */
    public TransactionInfo(long size, long timeReadNs, long tsNs, long scn) {
      _transactionSize = size;
      _transactionTimeReadNs = timeReadNs;
      _transactionEventTSNs = tsNs;
      _scn = scn;
    }

  }

  final private String _phSourceName;

  //errors
  private int _numErrors = 0;
  private int _numParsingErrors = 0;
  private int _numRestarts; //at this point we count number of parsing errors

  // file stats
  private int _filesAdded=0, _filesParsed=0;

  // read stats
  private long _bytesRead;
  private long _timeMostRecentRead;
  private long _timeMostRecentAdded;

  // lag info
  private long _bytesLag;
  private long _modTimeLag; // lag between ts of most recent added and parsed files
  private int _filesLag;

  // transaction info
  private int _numTransactions;
  private long _transactionSize;
  private long _transactionTimeReadNs;
  private long _mostRecentTransTSMs; // ts from transaction itself
  private long _maxScn; // maxSCN in transactions for the "subscribed" sources
  private int _transactionsWithEvents;
  private int _transactionsWithOutEvents;
  private int _eventsTotal; // events in transactions for the "subscribed" sources


  // SCN Regression related
  /**
   * NumSCNRegression counts the number of times SCN regressed ( went from a higher value to lower value )
   */
  private long _numScnRegressions = 0;
  /**
   * The regressed SCN which was seen latest during trail file processing.
   */
  private long _lastRegressedScn = -1;

  public GGParserStatistics(String phSourceName)
  {
    _phSourceName = phSourceName;
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder("\npSource=").append(_phSourceName);
    sb.append(";maxScn=").append(_maxScn);
    sb.append(";evTotal=").append(_eventsTotal);
    sb.append("\n");
    sb.append("filesAdded=").append(_filesAdded);
    sb.append(";filesParsed=").append(_filesParsed);
    sb.append(";bytesRead=").append(_bytesRead);
    sb.append(";TSMostRecentRead=").append(_timeMostRecentRead);
    sb.append(";TSMostRecentAdded=").append(_timeMostRecentAdded);
    sb.append(";tSinceRead=").append(getTimeSinceLastAccessMs());
    sb.append("\n");
    sb.append("LAG=").append(_bytesLag).append(":").append(_filesLag).append(":").append(_modTimeLag);
    sb.append("\n");
    sb.append("TxnTotal=").append(_transactionsWithEvents).append("+").append(_transactionsWithOutEvents).append("=").append(_numTransactions);
    sb.append("\n");
    sb.append("TxnStats(totalSize, readTime, lastTS)=").append(_transactionSize).append(":").append(_transactionTimeReadNs).append(":").append(_mostRecentTransTSMs);
    sb.append("\n");
    sb.append("parseErrofs=").append(_numParsingErrors).append(";errors=").append(_numErrors);
    sb.append("\n");
    sb.append("LastRegressSCN=").append(_lastRegressedScn).append(";NumSCNRegressions=").append(_numScnRegressions);

    return sb.toString();
  }

  /**
   * update on transaction read
   * @param size
   * @param timeRead
   * @param ts
   * @param scn
   */
  //public void addTransactionInfo(long transSize, long transTimeReadMs, long transTs, long transScn, int numEventsInTrans) {
  public void addTransactionInfo(TransactionInfo tr,  int numEventsInTrans) {
    _transactionSize += tr.getTransactionSize();
    _transactionTimeReadNs += tr.getTransactionTimeRead();
    _mostRecentTransTSMs = tr.getTransactionTimeStampNs() / DbusConstants.NUM_NSECS_IN_MSEC; //convert to MS
    long transScn = tr.getScn();
    if(transScn > _maxScn)
      _maxScn = transScn;
    if(numEventsInTrans > 0) {
      _transactionsWithEvents++;
    } else {
      _transactionsWithOutEvents++;
    }
    _eventsTotal += numEventsInTrans;
    _numTransactions++;
  }

  public void addError() {
    _numErrors ++;
  }

  public void addParsingError() {
    _numParsingErrors ++;
    _numRestarts ++;
  }

  /** update on file add
   * @param numNewFiles
   */
  public void addNewFile(int numNewFiles) {
    _filesAdded += numNewFiles;
  }

  /**
   * update on file parsed
   * @param files
   */
  public void addParsedFile(int files) {
    _filesParsed += files;
  }

  /**
   * update on read
   * @param bytesRead
   */
  public void addBytesParsed(long bytesParsed) {
    _bytesRead = bytesParsed;
    _timeMostRecentRead = System.currentTimeMillis();
  }

  /**
   * lag info update (bytes)
   * @param lag
   */
  public void setBytesLag(long lag) {
    _bytesLag = lag;
  }
  /**
   * lag info update (files)
   * @param lag
   */
  public void setFilesLag(int lag) {
    _filesLag = lag;
  }

  /**
   * lag info update (mod time)
   * @param lag
   */
  public void setModTimeLag(long tsBegin, long tsEnd) {
    _modTimeLag = tsEnd - tsBegin;
    _timeMostRecentAdded = tsEnd; // TS of the most recent available trail file
  }

  /////////////////Getters

  public String getPhysicalSourceName()
  {
    return _phSourceName;
  }

  @Override
  public int getNumTransactionsWithEvents()
  {
    return _transactionsWithEvents;
  }

  @Override
  public int getNumTransactionsWithoutEvents()
  {
    return _transactionsWithOutEvents;
  }

  @Override
  public int getNumTransactionsTotal()
  {
    return _numTransactions;
  }

  @Override
  public int getNumFilesParsed()
  {
    return _filesParsed;
  }
  @Override
  public int getNumFilesAdded()
  {
    return _filesAdded;
  }

  @Override
  public long getAvgTransactionSize()
  {
    if(_numTransactions == 0)
      return 0;

    return _transactionSize/_numTransactions;
  }

  @Override
  public long getAvgParseTransactionTimeNs()
  {
    if(_numTransactions == 0)
      return 0;

    return _transactionTimeReadNs/_numTransactions;
  }

  @Override
  public long getTimeSinceLastTransactionMs()
  {
    if(_mostRecentTransTSMs <= 0)
      return -1;

    return System.currentTimeMillis() - _mostRecentTransTSMs;
  }

  @Override
  public int getNumTotalEvents()
  {
    return _eventsTotal;
  }

  public long getMaxScn()
  {
    return _maxScn;
  }

  @Override
  public long getNumErrors()
  {
    return _numErrors;
  }

  @Override
  public long getNumParseErrors()
  {
    return _numParsingErrors;
  }

  @Override
  public long getNumParseRestarts()
  {
    return _numRestarts;
  }

  @Override
  public long getTimeSinceLastAccessMs()
  {
    if(_timeMostRecentRead == 0)
      return -1;
    return System.currentTimeMillis() - _timeMostRecentRead;
  }

  /**
   * number of files between files available and parsed
   * @see com.linkedin.databus.monitoring.mbean.GGParserStatisticsMBean#getNumFilesLag()
   */
  @Override
  public long getFilesLag()
  {
    return _filesLag;
  }

  /**
   * difference between modTime of the most recent file added and the one already parsed
   * @see com.linkedin.databus.monitoring.mbean.GGParserStatisticsMBean#getTimeLag()
   */
  @Override
  public long getTimeLag()
  {
    return _modTimeLag;
  }

  @Override
  public long getBytesLag()
  {
    return _bytesLag;
  }

  @Override
  public void reset()
  {
    _numErrors = 0;
    _numParsingErrors = 0;
    _numRestarts = 0;


    _filesAdded=0;
    _filesParsed=0;
    _bytesRead = 0;
    _timeMostRecentRead = 0;
    _timeMostRecentAdded = 0;


    _bytesLag = 0;
    _modTimeLag = 0;
    _filesLag = 0;

    _numTransactions = 0;
    _transactionSize = 0;
    _transactionTimeReadNs = 0;
    _mostRecentTransTSMs = 0;
    _maxScn = 0;
    _transactionsWithEvents = 0;
    _transactionsWithOutEvents = 0;
    _eventsTotal = 0;

    _numScnRegressions = 0;
    _lastRegressedScn = -1;
  }

  @Override
  public long getAvgFileSize()
  {
    return (_bytesRead + _bytesLag)/_filesAdded;
  }

  @Override
  public long getNumBytesTotalParsed() {
    return _bytesRead;
  }

  public long getTsMostRecentFileAdded()
  {
    if(_timeMostRecentAdded == 0)
      return -1;

    return _timeMostRecentAdded;
  }

  @Override
  public long getNumSCNRegressions()
  {
    return _numScnRegressions;
  }

  @Override
  public long getLastRegressedScn()
  {
    return _lastRegressedScn;
  }

  public void addScnRegression(long regressScn)
  {
    _numScnRegressions++;
    _lastRegressedScn = regressScn;
  }
}
