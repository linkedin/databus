/*
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
 */

package com.linkedin.databus2.producers.db;

import java.io.StringReader;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.xml.xpath.XPath;
import javax.xml.xpath.XPathConstants;
import javax.xml.xpath.XPathExpression;
import javax.xml.xpath.XPathExpressionException;
import javax.xml.xpath.XPathFactory;

import org.apache.log4j.Logger;
import org.w3c.dom.NodeList;
import org.xml.sax.InputSource;

import com.linkedin.databus.core.DatabusRuntimeException;
import com.linkedin.databus.core.ScnTxnPos;
import com.linkedin.databus.core.TrailFilePositionSetter;
import com.linkedin.databus.core.TrailFilePositionSetter.TransactionSCNFinderCallback;
import com.linkedin.databus.core.util.RateMonitor;
import com.linkedin.databus2.core.DatabusException;

/**
 *
 * Transaction callback that reads XML trail files, locating transaction boundaries and extracting SCN.
 *
 */
public class GGXMLTrailTransactionFinder implements TransactionSCNFinderCallback
{
  public static final String MODULE = GGXMLTrailTransactionFinder.class.getName();
  public static final Logger LOG = Logger.getLogger(MODULE);

  /**
   * The patterns for detecting SCN and transaction are selected in such a way that they won't be ambiguous.
   */
  public static final String TRANSACTION_BEGIN_PREFIX  = "<transaction"; // Uniquely identifies the transaction begin.
  public static final String TRANSACTION_END_PREFIX  = "</transaction";  // Uniquely identifies the transaction end

  public static final String TRANSACTION_END_STR  = "</transaction>";

  // X-Path expression for extracting SCN
  public static final String SCN_XPATH_STR = "//transaction/dbupdate/tokens/token[@name=\"TK-CSN\"]/text()";
  public static final String SCN_REGEX_STR = "(<token\\s+name=\"TK-CSN\"\\s*>([0-9]+)\\s*</token>)";
  public boolean _enableRegex = true;

  // Current Txn Position and SCN
  private ScnTxnPos _txnPos;

  // Prev Txn Position and SCN
  private ScnTxnPos _prevTxnPos;

  // For Tracking current cursor position
  private String _currFile;

  /** Byte offset within the file where the current cursor is */
  private long _currFileByteOffset;

  /** Line number of the cursor */
  private long _currLineNumber;

  /** The string buffer containing the current txn */
  private StringBuilder _currTxnStr = new StringBuilder();

  /** TargetSCN to be located */
  private long _targetScn = TrailFilePositionSetter.USE_LATEST_SCN;

  /** Flag to indicate if end of txn is seen */
  private boolean _txnEndSeen = false;

  /** Number of valid txns completely seen by this instance */
  private long _numTxnsSeen = 0;

  /** Number of invalid txns (i.e., containing no valid SCNs) seen by this instance */
  private long _numInvalidTxnsSeen = 0;

  /** Flag to indicate if at least one txn is completely seen */
  private boolean _firstTxnSeen = false;

  private final XPathExpression _expr;
  private final Pattern _rexpr;

  /** Member variables used in intermediate computations in regexQuery() / xpathQuery() */
  private long _minScn, _maxScn;

  private  RateMonitor _queryRateMonitor = new RateMonitor("Query_GGTransactionFinder");

  private  RateMonitor _rateMonitor = new RateMonitor("GGTransactionFinder");

  private boolean _beginTxnSeen = false;

  public GGXMLTrailTransactionFinder(boolean enableRegex) throws Exception
  {
    reset();
    XPathFactory xpathFactory = XPathFactory.newInstance();
    XPath xpath = xpathFactory.newXPath();
    _expr = xpath.compile(SCN_XPATH_STR);
    _rexpr = Pattern.compile(SCN_REGEX_STR);
    _enableRegex = enableRegex;
    _minScn = Long.MAX_VALUE;
    _maxScn = Long.MIN_VALUE;
  }

  public GGXMLTrailTransactionFinder() throws Exception
  {
    this(true);
  }

  @Override
  public void beginFileProcessing(String file)
  {
    if(LOG.isDebugEnabled())
     LOG.debug("Switching to file :" + file);
    _currFile = file;
    _currLineNumber = 0;
    _currFileByteOffset = 0;
  }

  @Override
  public boolean processLine(String line, int newLineCharLen) throws DatabusException
  {
    try
    {
      _rateMonitor.resume();
      _rateMonitor.ticks(line.length());
      String l = line;
      int totalOffset = 0; // tracks the byteOffset within the line on transaction beginning.
      boolean ret = false;

      /**
       * The general XML syntax allow for newlines to be optional between XML elements. Even though GG is empirically
       * shown to be inserting newlines between XML element tags, this assumption is not made here. A single line
       * can contain zero or more complete transactions.
       */
      int beginOffset = l.indexOf(TRANSACTION_BEGIN_PREFIX);
      int endOffset = l.indexOf(TRANSACTION_END_PREFIX);
      if ((beginOffset >= 0) || (endOffset >= 0))
      {
        /**
         * A transaction can be contained in a single or multiple lines. No assumptions should be made about its placement.
         */
        while (true)
        {
          _txnEndSeen = false;

          // Start and end of txns can be in a single line. Moreover, many such transactions be on a line.
          beginOffset = l.indexOf(TRANSACTION_BEGIN_PREFIX);
          endOffset = l.indexOf(TRANSACTION_END_PREFIX);

          if ( beginOffset >= 0)
            totalOffset += beginOffset;

          // no more endpoints (begin or end of transactions)
          if ( (endOffset == -1) && (beginOffset == -1))
            break;

          // Case where only beginning of transaction tag or a complete transaction is present
          if ( (endOffset == -1) ||
              ((beginOffset >= 0 ) && (beginOffset < endOffset)))
          {
            _currTxnStr.setLength(0);
            processBegin(totalOffset);

            if (endOffset == -1)
            {
              _currTxnStr.append(l);
              break;
            } else {
              _currTxnStr.append(l.subSequence(beginOffset, endOffset));
              _currTxnStr.append(TRANSACTION_END_STR);
              processEnd();
              totalOffset += endOffset + TRANSACTION_END_STR.length();
              l = l.substring(endOffset + TRANSACTION_END_STR.length());
            }
          } else if ( (beginOffset == -1) || (beginOffset > endOffset)) {
            // Case where only endTag is seen or a transaction completes and another starts in the same line
            if (beginOffset == -1)
            {
              _currTxnStr.append(l);
              processEnd();
              if (isDone())
                ret = true;
              break;  // nothing left to process on this line => must break unconditionally
            } else {
              _currTxnStr.append(l.subSequence(0, beginOffset));
              l = l.substring(beginOffset);
              processEnd();
            }
          }

          if (isDone())
          {
            ret = true;
            break;
          }
        }
      } else {
        _currTxnStr.append(l);  // continue accumulating "middle stuff" (between begin- and end-transaction tags)
      }
      _currFileByteOffset += line.length();

      if ( newLineCharLen > 0)
        _currFileByteOffset += newLineCharLen;

      _currLineNumber++;
      return ret;
    } finally {
      _rateMonitor.suspend();
    }
  }

  private boolean isDone()
  {
    // last condition works only because code elsewhere has checked for _targetScn >= min SCN ?
    if (_txnEndSeen &&
        ((_targetScn == TrailFilePositionSetter.USE_EARLIEST_SCN)
           || (( _targetScn != TrailFilePositionSetter.USE_LATEST_SCN) && (_txnPos.getMaxScn() >= _targetScn))))
    {
      return true;
    }

    return false;
  }

  /**
   * When transaction begin is seen, this should be called to save the positions
   * @param byteLineOffset
   */
  private void processBegin(int byteLineOffset)
  {
    _prevTxnPos.copyFrom(_txnPos);
    _txnPos.setFile(_currFile);
    _txnPos.setFileOffset(_currFileByteOffset + byteLineOffset);
    _txnPos.setLineNumber(_currLineNumber+1);
    _txnPos.setLineOffset(byteLineOffset);
    _txnPos.setMinScn(-1);
    _txnPos.setMaxScn(-1);
    _beginTxnSeen = true;
  }

  private void xpathQuery() throws DatabusTrailFileParseException
  {
    try
    {
      //Set SCN
      InputSource source = new InputSource(new StringReader(_currTxnStr.toString()));

      _queryRateMonitor.resume();  // count time consumed by XML parsing
      Object result = _expr.evaluate(source, XPathConstants.NODESET);
      _queryRateMonitor.ticks(_currTxnStr.length());
      _queryRateMonitor.suspend();

      NodeList nodes = (NodeList) result;
      for (int i = 0; i < nodes.getLength(); i++)
      {
        long newScn = Long.parseLong((nodes.item(i).getNodeValue().trim()));
        _minScn = Math.min(_minScn, newScn);
        _maxScn = Math.max(_maxScn, newScn);
      }
    }
    catch (XPathExpressionException xpxe)
    {
      throw new DatabusTrailFileParseException("Got XPath exception for trail-file entry: " + _currTxnStr, xpxe);
    }
    catch (NumberFormatException nfe)
    {
      throw new DatabusTrailFileParseException("Got parseLong() exception for trail-file entry: " + _currTxnStr, nfe);
    }
  }

  private void regexQuery() throws DatabusTrailFileParseException
  {
    String source = _currTxnStr.toString();

    _queryRateMonitor.resume();  // count time consumed by regex parsing
    Matcher result = _rexpr.matcher(source);
    boolean foundScn = result.find();
    _queryRateMonitor.ticks(source.length());
    _queryRateMonitor.suspend();

    if (!foundScn)
    {
      throw new DatabusTrailFileParseException("Could not find TK-SCN with regex; " +
                                               "likely error in trail-file entry: " + _currTxnStr);
    }

    // Loop through all SCNs in the transaction and save max/min ones.
    while (foundScn)
    {
      String m = result.group(2);
      long newScn = Long.parseLong(m);  // TODO:  try/catch?  regex will catch most errors, but NumberFormatException still ~possible
      _minScn = Math.min(_minScn, newScn);
      _maxScn = Math.max(_maxScn, newScn);
      _queryRateMonitor.resume();  // also count time consumed by regex find() calls
      foundScn = result.find();
      _queryRateMonitor.suspend();
    }
  }

  /**
   * When the transaction end is seen, this should be called to save SCN
   * @throws DatabusException
   */
  private void processEnd() throws DatabusException
  {
    if (! _beginTxnSeen)
    {
      _currTxnStr.setLength(0);
      return;
    }

    _maxScn = Long.valueOf(-1);
    _minScn = Long.MAX_VALUE;

    try
    {
      if (!_enableRegex)
      {
        xpathQuery();
      }
      else
      {
        regexQuery();
      }
    }
    catch (DatabusTrailFileParseException ex)
    {
      LOG.warn("empty/corrupted txn (" + ex.getMessage() + "); resetting invalid _txnPos (" + _txnPos +
               ") to _prevTxnPos (" + _prevTxnPos + ")");
      _txnPos.copyFrom(_prevTxnPos);
      ++_numInvalidTxnsSeen;  // TODO:  wire into metrics/monitoring (need accessor plus whatever lies on caller's end)
      return;
    }

    _txnPos.setMaxScn(_maxScn);
    _txnPos.setMinScn(_minScn);
    _txnEndSeen = true;
    _numTxnsSeen++;

    if (! _firstTxnSeen )
    {
      if ((_targetScn >= 0) && (_targetScn < _minScn))  // common case:  need to try previous trail file instead
        throw new DatabusException("SinceSCN is less than MinScn available in trail file. Requested SinceSCN is :"
            + _targetScn + " but found only : " + _minScn
            + " in Location " + _txnPos);
    }
    _firstTxnSeen = true;
    _beginTxnSeen = false;
    if (LOG.isDebugEnabled())
    {
      LOG.debug("Seen Txn : " + _txnPos);
    }
  }

  @Override
  public void endFileProcessing(String file)
  {
  }

  @Override
  public ScnTxnPos getTxnPos()
  {
    if (_txnPos.isEmpty() && _prevTxnPos.isEmpty())
      return null;

    if (_txnPos.isEmpty())
      return _prevTxnPos;

    return _txnPos;
  }

  @Override
  public void reset()
  {
    _txnPos = new ScnTxnPos();
    _prevTxnPos = new ScnTxnPos();
    _currFile = null;
    _currFileByteOffset = 0;
    _currLineNumber = 0;
    _numTxnsSeen = 0;
    _numInvalidTxnsSeen = 0;
    _txnEndSeen = false;
    _beginTxnSeen = false;
    _firstTxnSeen = false;
    _currTxnStr.setLength(0);
    _queryRateMonitor = new RateMonitor("XPath_GGTransactionFinder");
    _queryRateMonitor.start();
    _queryRateMonitor.suspend();

    _rateMonitor = new RateMonitor("GGTransactionFinder");
    _rateMonitor.start();
    _rateMonitor.suspend();
  }

  @Override
  public void begin(long targetScn)
  {
    _targetScn = targetScn;
  }

  @Override
  public long getNumTxnsSeen()
  {
    return _numTxnsSeen;
  }

  @Override
  public long getCurrentFileOffset()
  {
    return _currFileByteOffset;
  }

  public RateMonitor getQueryRateMonitor()
  {
	  return _queryRateMonitor;
  }

  public RateMonitor getRateMonitor()
  {
      return _queryRateMonitor;
  }

  @Override
  public String getPerfStats()
  {
    String overallRateMonitor = _rateMonitor.toString();
    String queryRateMonitor  = _queryRateMonitor.toString();

    // TODO Auto-generated method stub
    return "queryRM : " + queryRateMonitor +
           ", OverallRM : " + overallRateMonitor;
  }

  /** Special-purpose exception used only by processEnd() and the xpath and regex parsers. */
  private class DatabusTrailFileParseException extends Exception
  {
    public DatabusTrailFileParseException()
    {
      super();
    }

    public DatabusTrailFileParseException(String message, Throwable cause)
    {
      super(message, cause);
    }

    public DatabusTrailFileParseException(String message)
    {
      super(message);
    }

    public DatabusTrailFileParseException(Throwable cause)
    {
      super(cause);
    }
  }
}
