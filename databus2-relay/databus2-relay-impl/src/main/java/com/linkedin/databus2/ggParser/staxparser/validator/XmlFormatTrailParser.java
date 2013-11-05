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
package com.linkedin.databus2.ggParser.staxparser.validator;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.PrintStream;
import java.io.RandomAccessFile;
import java.io.SequenceInputStream;
import java.nio.charset.Charset;
import java.util.Arrays;
import java.util.Collections;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;

import javax.xml.stream.FactoryConfigurationError;
import javax.xml.stream.XMLInputFactory;
import javax.xml.stream.XMLStreamConstants;
import javax.xml.stream.XMLStreamException;
import javax.xml.stream.XMLStreamReader;

import org.apache.log4j.Logger;

import com.linkedin.databus.core.ConcurrentAppendableCompositeFileInputStream;
import com.linkedin.databus2.ggParser.XmlStateMachine.ColumnState;
import com.linkedin.databus2.ggParser.XmlStateMachine.DbUpdateState;
import com.linkedin.databus2.ggParser.XmlStateMachine.TokenState;

/**
 * A parser that reads XMLFORMAT GoldenGate trail file and validates it.
 */
public class XmlFormatTrailParser implements Runnable
{
  private static final long POSITION_REPORT_GAP = 10L * 1024L * 1024;
  private static final long POSITION_REPORT_TIME_MS = 30 * 1000;
  private static final int ERROR_CONTEXT_LEN = 100;

  private static final String TOKEN_XID = "TK-XID";

  public static final String DTD =
      "<!DOCTYPE root [ \n" +
        "<!ELEMENT root (transaction)*> \n" +
        "<!ELEMENT transaction (dbupdate)*> \n" +
        "<!ATTLIST transaction \n" +
        "    timestamp CDATA #REQUIRED \n" +
        ">\n" +
        "<!ELEMENT dbupdate (columns, tokens)> \n" +
        "<!ATTLIST dbupdate \n" +
        "    table CDATA #REQUIRED \n" +
        "    type  (insert|delete|update) #REQUIRED \n" +
        ">\n" +
        "<!ELEMENT columns (column)+>\n" +
        "<!ELEMENT column (#PCDATA)>\n" +
        "<!ATTLIST column \n" +
        "    name   CDATA #REQUIRED \n" +
        "    key    (true) #IMPLIED \n" +
        "    status CDATA #IMPLIED \n" +
        ">\n" +
        "<!ELEMENT tokens (token)+>\n" +
        "<!ELEMENT token (#PCDATA)>\n" +
        "<!ATTLIST token \n" +
        "    name CDATA #REQUIRED \n" +
        ">\n" +
      "]>\n";

  private static enum DbupdateType
  {
    INSERT,
    DELETE,
    UPDATE
  };

  private final Logger _log;
  /** The STAX XML reader factory*/
  private final XMLInputFactory _xmlInputFactory;
  /** The STAX XML reader */
  private final XMLStreamReader _xmlStreamReader;
  /** The input stream that coaslesces all trail files into a single stream*/
  private final ConcurrentAppendableCompositeFileInputStream _inputStream;
  /** A hack to add missing XML root element in the trails */
  private final InputStream _realInputStream;
  /** A flag to shutdown the parsing */
  private final AtomicBoolean _shutdownRequested = new AtomicBoolean(false);
  /** The last observed error */
  private Throwable _lastError = null;
  /** The currently processed trail file name */
  private String _lastFileName = null;
  /** The offset of the last block of data read by the XML reader because it does
   * internal buffering */
  private long _lastPosition = 0;
  /** The state of the parser */
  private String _currentTableName;
  /** A map from dbname.tablename to a set of column names we've seen for that table */
  private Map<String, Set<String>> _tableColumns = new HashMap<String, Set<String>>();
  /** The set of columns we've seen for the current table */
  private Set<String> _curTableColumns = new HashSet<String>();
  /** The set of GG tokens we've seen for the current table */
  private Set<String> _curTableTokens = new HashSet<String>();
  /** The type of the current <dbupdate> */
  private DbupdateType _dbupdateType;
  private final boolean _continueOnError;
  private final PrintStream _errOut;
  private long _errorCount = 0;

  public XmlFormatTrailParser(ConcurrentAppendableCompositeFileInputStream inputStream,
                              boolean validating,
                              Logger log,
                              String errorLogFile) throws XMLStreamException, IOException
  {
    super();
    _log = (null == log) ? Logger.getLogger(XmlFormatTrailParser.class) : log;
    _inputStream = inputStream;
    _realInputStream = wrapStreamWithXmlTags(_inputStream);
    _xmlInputFactory =  createXmlInputFactory(validating);
    _xmlInputFactory.setProperty(XMLInputFactory.IS_COALESCING, Boolean.TRUE);
    _xmlInputFactory.setProperty(XMLInputFactory.SUPPORT_DTD, Boolean.TRUE);
    _xmlStreamReader = _xmlInputFactory.createXMLStreamReader(_realInputStream);
    _continueOnError = null != errorLogFile;
    if (_continueOnError)
    {
      _errOut = new PrintStream(errorLogFile);
    }
    else
    {
      _errOut = null;
    }
  }

  private XMLInputFactory createXmlInputFactory(boolean validating)
      throws FactoryConfigurationError
  {
    XMLInputFactory result = null;
    Throwable createError = null;
    try
    {
      @SuppressWarnings("unchecked")
      Class<XMLInputFactory> woodstoxFactory =
          (Class<XMLInputFactory>)Class.forName("com.ctc.wstx.stax.WstxInputFactory");
      result = woodstoxFactory.newInstance();
      if (validating)
      {
        _log.info("found woodstox library: DTD validation will be enabled");
        result.setProperty(XMLInputFactory.IS_VALIDATING, validating);
      }
    }
    catch (ClassNotFoundException e)
    {
      createError = e;
    }
    catch (InstantiationException e)
    {
      createError = e;
    }
    catch (IllegalAccessException e)
    {
      createError = e;
    }
    catch (RuntimeException e)
    {
      createError = e;
    }

    if (null != createError)
    {
      _log.info("unable to find woodstox library, defaulting to Java: " + createError);
      if (validating)
      {
        _log.warn("default implementation does not support DTD validation");
      }
      result = XMLInputFactory.newInstance();
    }

    return result;
  }

  /** Make the trail files input look like real XML */
  private InputStream wrapStreamWithXmlTags(InputStream compositeInputStream)
  {

    String xmlStart = "<?xml version=\"1.0\" encoding=\"ISO-8859-1\"?>\n" + DTD + "\n<root>";
    String xmlEnd = "</root>";
    _log.info("The xml start tag used is:" + xmlStart);
    List<InputStream> xmlTagsList = Arrays.asList(new InputStream[]
                                         {
                                             new ByteArrayInputStream(xmlStart.getBytes(Charset.forName("ISO-8859-1"))),
                                             compositeInputStream,
                                             new ByteArrayInputStream(xmlEnd.getBytes(Charset.forName("ISO-8859-1"))),
                                         });
    Enumeration<InputStream> streams = Collections.enumeration(xmlTagsList);
    SequenceInputStream seqStream = new SequenceInputStream(streams);
    return seqStream;
  }

  public void shutdownAsyncronously()
  {
    _shutdownRequested.set(true);
  }

  /**
   * @see java.lang.Runnable#run()
   */
  @Override
  public void run()
  {
    long lastReportTs = 0;
    long lastReportedPosition = _lastPosition;
    long savePos = _lastPosition;
    try
    {
      while (! _shutdownRequested.get() && _xmlStreamReader.hasNext())
      {
        int eventType = _xmlStreamReader.next();
        switch (eventType)
        {
        case XMLStreamConstants.START_ELEMENT: processStartElement(); break;
        case XMLStreamConstants.END_ELEMENT: processEndElement(); break;
//        case XMLStreamConstants.ATTRIBUTE: processAttribute(); break;
        default: break; //do nothing -- just log progress
        }

        File curFile = _inputStream.getCurrentFile();
        if (null != curFile && ! _inputStream.isClosed())
        {
          String curFileName = curFile.getName();
          long currentTs = System.currentTimeMillis();
          long curPos = _inputStream.getCurrentPosition();
          if (curPos != savePos)
          {
            //the XML reader seems to do internal buffering which makes it harder to
            //guess the position where a problem happened.
            //At any point the parser is processing between the bytes [_lastPosition, curPos).
            _lastPosition = savePos;
            savePos = curPos;
          }
          if (! curFileName.equals(_lastFileName) ||
              (curPos - lastReportedPosition) >= POSITION_REPORT_GAP ||
              (currentTs - lastReportTs) >= POSITION_REPORT_TIME_MS)
          {
            lastReportedPosition = curPos;
            lastReportTs = currentTs;
            _log.info("file: " + curFileName +  "; pos: " +  curPos);
            logTablesSeen();
          }
          _lastFileName = curFileName;
        }
      }
    }
    catch (XMLStreamException e)
    {
      _log.error("xml stream error: " + e, e);
      _lastError = e;
    }
    catch (RuntimeException e)
    {
      _log.error("runtime error: " + e, e);
      _lastError = e;
    }
  }

  private void processError(String msg, Throwable e)
  {
    ++_errorCount;
    if (_continueOnError)
    {
      _log.error("PARSE ERROR: " + msg);
      _errOut.println("=========================================");
      _errOut.println(String.format("PARSE ERROR %s", msg) );
      try
      {
        printErrorContext(_errOut);
      }
      catch (IOException e1)
      {
        _errOut.println("I/O error: " + e1);
      }
      _errOut.println("=========================================");
    }
    else
    {
      if (null != e)
      {
        throw new RuntimeException(msg);
      }
      else
      {
        throw new RuntimeException(msg, e);
      }
    }
  }

  /** In case an error print out the trail file context. Because of the
   * XML reader buffering, we can only get the block (8K). It has to be
   * visually inspected */
  protected void printErrorContext(PrintStream errOut) throws IOException
  {
    final File file = _inputStream.getCurrentFile();
    final long position = _inputStream.getCurrentPosition();
    File lastFile = new File(file.getParentFile(), getLastFileName());
    errOut.println("error between " + lastFile + " @ " + getLastPosition() +
              " and " + _inputStream.getCurrentFile() +
              " @ " + _inputStream.getCurrentPosition());
    RandomAccessFile f = new RandomAccessFile(file, "r");
    try
    {
      long startPos = lastFile.equals(file) ? getLastPosition() : 0;
      long endPos = position + ERROR_CONTEXT_LEN;
      int contextSize = (int)(endPos - startPos);
      byte[] context = new byte[contextSize];
      f.seek(startPos);
      if (f.read(context, 0, contextSize) > 0)
      {
        errOut.println("context: " + new String(context, "ISO-8859-1"));
      }
      else
      {
        errOut.println("unable to read XML error context");
      }
    }
    finally
    {
      f.close();
    }
  }

  /**
   * Logs the currently discovered tables
   */
  private void logTablesSeen()
  {
    if (_log.isInfoEnabled())
    {
      _log.info("Discovered tables:" + _tableColumns);
    }
  }

  /**
   * Processes a GG token for the current table update
   */
  private void processToken(String tokenName)
  {
    _curTableTokens.add(tokenName);
    if (_log.isDebugEnabled())
    {
      _log.debug("added token " + tokenName + " for table " + _currentTableName);
    }
  }

  /**
   * Processes a new column for the current table update
   */
  private void processColumn(String columnName, boolean isKey)
  {
    if (isKey)
    {
      _curTableColumns.add("*" + columnName + "*");
    }
    else
    {
      _curTableColumns.add(columnName);
    }
    if (_log.isDebugEnabled())
    {
      _log.debug("added column " + columnName + " for table " + _currentTableName);
    }

  }

  /**
   *
   */
  private void processEndElement()
  {
    final String elemName = _xmlStreamReader.getLocalName();
    if (ColumnState.COLUMNSTATE.equals(elemName))
    {
      endColumnElement();
    }
    else if (TokenState.TOKENSTATE.equals(elemName))
    {
      endTokenElement();
    }
    else if (DbUpdateState.DBUPDATE.equals(elemName))
    {
      endDbupdateElement();
    }
  }

  /**
   * End processing a <dbupdate> element
   */
  private void endDbupdateElement()
  {
    validateTokens();
    validateColumns();
    _dbupdateType = null;
  }

  /**
   * Validate the columns we've seen for a table update:
   * (1) GG-specific columns are present
   *
   * <p>The method also keeps track of all columns we've seen for a table
   */
  private void validateColumns()
  {
    if (_log.isDebugEnabled())
    {
      _log.debug("validating columns " + _curTableColumns + " for table " + _currentTableName);
    }
    /** GG_STATUS and GG_MODI_TS may or may not be present for delete operations */
    if (DbupdateType.DELETE != _dbupdateType)
    {
      if (! _curTableColumns.contains("GG_STATUS"))
      {
        processError("missing GG_STATUS column for table " + _currentTableName + " update type:" + _dbupdateType, null);
      }
      if (! _curTableColumns.contains("GG_MODI_TS"))
      {
        processError("missing column GG_MODI_TS for table " + _currentTableName + " update type:" + _dbupdateType, null);
      }
    }

    if (_tableColumns.containsKey(_currentTableName))
    {
      Set<String> cols = _tableColumns.get(_currentTableName);
      cols.addAll(_curTableColumns);
    }
    else
    {
      _tableColumns.put(_currentTableName, new HashSet<String>(_curTableColumns));
    }
  }

  /**
   * Validates the token for the table update:
   * (1) make sure that the transaction id is there
   * (2) make sure that the SCN is there
   */
  private void validateTokens()
  {
    if (_log.isDebugEnabled())
    {
      _log.debug("validating tokens " + _curTableTokens + " for table " + _currentTableName);
    }
    if (! _curTableTokens.contains(TOKEN_XID))
    {
      processError("missing transaction id TK-XID for table "  + _currentTableName, null);
    }
    if (! _curTableTokens.contains(TokenState.TOKENSCN))
    {
      processError("missing SCN TK-CSN for table "  + _currentTableName, null);
    }
  }

  /**
   * End processing a <token> element
   */
  private void endTokenElement()
  {
  }

  /**
   * End processing a <column> element
   */
  private void endColumnElement()
  {
  }

  /** Process the start of an XML element - see if it is a element we care about */
  private void processStartElement()
  {
    final String elemName = _xmlStreamReader.getLocalName();
    if (ColumnState.COLUMNSTATE.equals(elemName))
    {
      startColumnElement();
    }
    else if (TokenState.TOKENSTATE.equals(elemName))
    {
      startTokenElement();
    }
    else if (DbUpdateState.DBUPDATE.equals(elemName))
    {
      startDbupdateElement();
    }
  }

  /**
   * Start processing a <dbupdate> element
   */
  private void startDbupdateElement()
  {
    _curTableColumns.clear();
    _curTableTokens.clear();
    _currentTableName = _xmlStreamReader.getAttributeValue(null, DbUpdateState.TABLEATTR);
    String dbupdateTypeStr = _xmlStreamReader.getAttributeValue(null, DbUpdateState.UPDATEATTRNAME);
    if (null == dbupdateTypeStr)
    {
      processError("missing type for <dbupdate> element for table " + _currentTableName, null);
    }
    try
    {
      _dbupdateType = DbupdateType.valueOf(dbupdateTypeStr.toUpperCase());
    }
    catch (IllegalArgumentException e)
    {
      processError("unknown <dbupdate> type:" + dbupdateTypeStr, null);
    }
  }

  /**
   * Start processing a <token> element
   */
  private void startTokenElement()
  {
    processToken(_xmlStreamReader.getAttributeValue(null, TokenState.TOKENATTRNAME));
  }

  /**
   * Start processing a <column> element
   */
  private void startColumnElement()
  {
    processColumn(_xmlStreamReader.getAttributeValue(null, ColumnState.FIELDNAMEATTR),
                  null != _xmlStreamReader.getAttributeValue(null, ColumnState.KEYNAMEATTR));
  }

  public Logger getLog()
  {
    return _log;
  }

  /**
   * Last parsing or validation error
   */
  public Throwable getLastError()
  {
    return _lastError;
  }

  /**
   * @return the lastPosition
   */
  public long getLastPosition()
  {
    return _lastPosition;
  }

  /**
   * @return the lastFileName
   */
  public String getLastFileName()
  {
    return _lastFileName;
  }

  /**
   * @return the errorCount
   */
  public long getErrorCount()
  {
    return _errorCount;
  }


}
