package com.linkedin.databus2.tools.console;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStreamWriter;
import java.io.PrintStream;
import java.util.Arrays;

import jline.ANSIBuffer;
import jline.ConsoleReader;

import org.jboss.netty.channel.group.ChannelGroup;
import org.jboss.netty.channel.group.DefaultChannelGroup;

import com.linkedin.databus.core.DbusEvent;
import com.linkedin.databus.core.util.ConfigBuilder;
import com.linkedin.databus.core.util.InvalidConfigException;
import com.linkedin.databus2.core.container.request.BinaryProtocol;

public class DatabusConsoleMain
{
  public static enum Compatibility
  {
    V2,
    ESPRESSO
  }

  public static class StaticConfig
  {
    private final Compatibility _compatibility;
    private final boolean _color;

    public StaticConfig(Compatibility compatibility, boolean color)
    {
      _compatibility = compatibility;
      _color = color;
    }

    public Compatibility getCompatibility() {return _compatibility;}

    public boolean isColor() { return _color; }
  }

  public static class StaticConfigBuilder implements ConfigBuilder<StaticConfig>
  {
    private String _compatibility = Compatibility.ESPRESSO.toString();
    private boolean _color = false;

    @Override
    public StaticConfig build() throws InvalidConfigException
    {
      Compatibility compatibility = null;
      try
      {
        compatibility = Compatibility.valueOf(_compatibility);
      }
      catch (RuntimeException e)
      {
        throw new InvalidConfigException("invalid compatibility:" + _compatibility);
      }

      return new StaticConfig(compatibility, _color);
    }

    public String getCompatibility() { return _compatibility; }

    public void setCompatibility(String compatibility) { _compatibility = compatibility; }

    public boolean isColor()
    {
      return _color;
    }

    public void setColor(boolean color)
    {
      _color = color;
    }

  }

  private final InputStream _cmdIn;
  private final PrintStream _cmdOut;
  private final ConsoleReader _consoleReader;
  private final StaticConfig _config;
  private boolean _exitRequested = false;
  private EspressoTcpClientConnection _etcpConn;
  private final ChannelGroup _channelGroup;

  public DatabusConsoleMain(StaticConfig conf) throws IOException
  {
    this(System.in, System.out, conf);
  }

  public DatabusConsoleMain(InputStream cmdIn, PrintStream cmdOut, StaticConfig conf) throws IOException
  {
    _cmdIn = cmdIn;
    _cmdOut = cmdOut;
    _consoleReader = new ConsoleReader(_cmdIn, new OutputStreamWriter(_cmdOut));
    _consoleReader.setDefaultPrompt("databus>");
    _config = conf;
    _channelGroup = new DefaultChannelGroup();

    _etcpConn = new EspressoTcpClientConnection(_channelGroup);
  }

  public void run() throws Exception
  {
    String line = null;
    try
    {
      while (!_exitRequested && ((line = _consoleReader.readLine()) != null))
      {
        String[] terms = line.split(" +");
        processLine(terms);
      }
    }
    finally
    {
      _etcpConn.cleanup();
    }
  }

  private void processLine(String[] line)
  {
    if (line[0].equals("exit") || line[0].equals("quit") || line[0].equals("bye")) _exitRequested = true;
    else if (line[0].equals("connect")) processConnect(line, 1);
    else if (line[0].equals("show")) processShow(line, 1);
    else if (line[0].equals("disconnect")) processDisconnect(line, 1);
    else if (line[0].equals("getLastSequence")) processGetLastSequence(line, 1);
    else printError("unkown command: " + Arrays.toString(line));
  }

  private void processGetLastSequence(String[] line, int i)
  {
  }

  private void printError(String string)
  {
    String realMessage = "error:" + string + "\n";
    if (_config.isColor() && _consoleReader.getTerminal().isANSISupported())
    {
      ANSIBuffer ansiBuffer = new ANSIBuffer();
      ansiBuffer.red(realMessage);
      realMessage = ansiBuffer.toString();
    }
    try
    {
      _consoleReader.printString(realMessage);
    }
    catch (IOException e)
    {
      e.printStackTrace();
    }
  }

  private void printStatus(String string)
  {
    try
    {
      _consoleReader.printString(string + "\n");
    }
    catch (IOException e)
    {
      e.printStackTrace();
    }
  }

  private void processConnect(String[] line, int idx)
  {
    if (line[idx].equals("tcp")) processConnectTcp(line, idx + 1);
    else printError("unknown connect: " + Arrays.toString(line));
  }

  private void processDisconnect(String[] line, int idx)
  {
    if (line[idx].equals("tcp")) processDisconnectTcp(line, idx + 1);
    else printError("unknown disconnect: " + Arrays.toString(line));
  }

  private void processConnectTcp(String[] line, int idx)
  {
    if (Compatibility.ESPRESSO == _config.getCompatibility()) processConnectTcpEspresso(line, idx);
  }

  private void processDisconnectTcp(String[] line, int idx)
  {
    if (Compatibility.ESPRESSO == _config.getCompatibility()) processDisconnectTcpEspresso(line, idx);
  }

  private void processConnectTcpEspresso(String[] line, int idx)
  {
    _etcpConn.connect(line[idx]);
    printStatus(_etcpConn.toString());
  }

  private void processDisconnectTcpEspresso(String[] line, int idx)
  {
    _etcpConn.disconnect();
    printStatus(_etcpConn.toString());
  }

  private void processShow(String[] line, int idx)
  {
    if (line[idx].equals("conns")) processShowConnections(line, idx + 1);
    else printError("unknown show: " + Arrays.toString(line));
  }

  private void processShowConnections(String[] line, int idx)
  {
    printStatus("espresso TCP: " + _etcpConn);
  }

  public static void main(String[] args) throws Exception
  {
    StaticConfigBuilder configBuilder = new StaticConfigBuilder();

    StaticConfig config = configBuilder.build();
    if (Compatibility.ESPRESSO == config.getCompatibility()) DbusEvent.byteOrder = BinaryProtocol.BYTE_ORDER;
    DatabusConsoleMain console = new DatabusConsoleMain(config);
    console.run();
  }

}
