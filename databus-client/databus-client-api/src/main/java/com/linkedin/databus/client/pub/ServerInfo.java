package com.linkedin.databus.client.pub;

import java.io.StringWriter;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

import org.apache.log4j.Logger;
import org.codehaus.jackson.map.ObjectMapper;

import com.linkedin.databus.client.pub.DatabusServerCoordinates.StateId;
import com.linkedin.databus.core.data_model.DatabusSubscription;
import com.linkedin.databus.core.data_model.LegacySubscriptionUriCodec;
import com.linkedin.databus.core.data_model.SubscriptionUriCodec;
import com.linkedin.databus.core.util.ConfigBuilder;
import com.linkedin.databus.core.util.InvalidConfigException;
import com.linkedin.databus2.core.DatabusException;

/**
 * Contains address and sources/subscriptions supported by a Databus server (relay or bootstrap
 * server)*/
public class ServerInfo implements Comparable<ServerInfo>
{
  public static final String      MODULE = ServerInfo.class.getName();
  public static final Logger      LOG    = Logger.getLogger(MODULE);

  /**
   * Describes where the relay is located ( ip address, port, friendly name and id associated )
   */
  protected final DatabusServerCoordinates _serverCoordinates;

  /**
   * The list of sources the client is interested in
   * Example : In Espresso (V3) it could be BizProfile.BizCompany
   *           In Databus V2, it could be BizFollow or its fully qualified database name (com.linkedin.xxx)
   */
  protected final List<String>      		_sources;

  /** A list of subscriptions supported by the server. This supersedes {@link #_sources} for V3. */
  protected final List<DatabusSubscription> _subs;

  protected String                  _jsonString;

    /* The name of the physical source */
  private String _PhysicalSourceName = "DefaultPhysicalSource";

  /**
   * Constructor
   * @param name        a human-readable name for he server
   * @param state       availability of the server {@link StateId}
   * @param address     the server address
   * @param sources     the list of sources hosted by the server
   */
  public ServerInfo(String name, String state, InetSocketAddress address, List<String> sources)
  {
    super();
    _serverCoordinates = new DatabusServerCoordinates(name, address, state);
    _sources = sources;
    List<DatabusSubscription> subsList = null;
    try
    {
      subsList = DatabusSubscription.createFromUriList(sources);
    }
    catch (DatabusException e)
    {
      LOG.warn("unable to parse sources list: " + e.getMessage(), e);
      subsList = Collections.emptyList();
    }
    catch (URISyntaxException e)
    {
      LOG.warn("unable to parse sources list: " + e.getMessage(), e);
      subsList = Collections.emptyList();
    }
    _subs = subsList;
  }

  /**
   * Constructor
   * @param name        a human-readable name for he server
   * @param state       availability of the server {@link StateId}
   * @param address     the server address
   * @param subs        the list of sources hosted by the server
   * @param uriCode     an option codec to populate the _sources field
   */
  public ServerInfo(String name, StateId state, InetSocketAddress address,
                    Collection<DatabusSubscription> subs,
                    SubscriptionUriCodec uriCodec)
  {
    super();
    _serverCoordinates = new DatabusServerCoordinates(name, address, state);
    _subs = new ArrayList<DatabusSubscription>(subs);
    if (null == uriCodec)
    {
      _sources = DatabusSubscription.createUriStringList(_subs,
                                                         LegacySubscriptionUriCodec.getInstance());
    }
    else
    {
      _sources = new ArrayList<String>(subs.size());
      for (DatabusSubscription sub: subs)
      {
        String uri = uriCodec.encode(sub).toString();
        _sources.add(uri);
      }
    }
  }

  /**
   * Constructor
   * @param name        a human-readable name for he server
   * @param state       availability of the server {@link StateId}
   * @param address     the server address
   * @param sources     the list of sources hosted by the server
   */
  public ServerInfo(String name, String state, InetSocketAddress address, String... sources)
  {
    this(name, state, address, Arrays.asList(sources));
  }


	/**
	* The name of the physical source that is hosted by the relay
	*/
	public String getPhysicalSourceName() {
		return _PhysicalSourceName;
	}

	public void setPhysicalSourceName(String PhysicalSourceName) {
		_PhysicalSourceName = PhysicalSourceName;
	}


  /**
   * A name that identifies the server (relay, bootstrap-server)
   */

  public String getName()
  {
    return _serverCoordinates.getName();
  }

  /**
   * The address of the server (relay, bootstrap-server)
   */
  public InetSocketAddress getAddress()
  {
    return _serverCoordinates.getAddress();
  }

  /**
   * The sources supported by the server (Relay, bootstrap-server)
   */
  public List<String> getSources()
  {
    return _sources;
  }

  /** The list of subscriptions supported by he server */
  public List<DatabusSubscription> getSubs()
  {
    return _subs;
  }

  public String toJsonString()
  {
    if (null == _jsonString)
    {
      StringWriter out = new StringWriter();
      ObjectMapper objMapper = new ObjectMapper();
      try
      {
        objMapper.writeValue(out, this);
        _jsonString = out.toString();
      }
      catch (Exception e)
      {
        _jsonString = "serialiationError";
      }
    }

    return _jsonString;
  }

  @Override
  public String toString()
  {
    return toJsonString();
  }

  /**
   * Checks if the server supports a list of sources. Order is significant
   * @param sources   the list of source to check
   * @return true iff the server can serve the sources
   */
  public boolean supportsSources(List<String> sources)
  {
    return checkSubsequence(sources, getSources());
  }

  /**
   * Checks if the first list of sources is a sub-sequence of the second list of server sources
   * @param sources         the list of sources to check
   * @param serverSources   the server sources to check against
   * @return true iff sources is a subsequence of serverSources
   */
  public static boolean checkSubsequence(List<String> sources, List<String> serverSources)
  {
      int maxPos = 0;
      for (String source : sources)
      {
          for (; maxPos < serverSources.size() && !serverSources.get(maxPos).equals(source); ++maxPos)
              ;
          if (maxPos == serverSources.size())
          {
              return false;
          }
      }

      return true;
  }

  /**
   * Checks if the first list of subscriptions is a sub-sequence of the second list of server subscriptions
   * @param subs         the list of subscriptions to check
   * @param serverSubs   the server subscriptions to check against
   * @return true iff sources is a subsequence of serverSources
   */
  public static boolean checkSubsequenceSubsV3(List<DatabusSubscription> subs,
                                               List<DatabusSubscription> serverSubs)
  {
    int maxPos = 0;
    for (DatabusSubscription sub : subs)
    {
      for (; maxPos < serverSubs.size() && !serverSubs.get(maxPos).equals(sub); ++maxPos)
        ;
      if (maxPos == serverSubs.size())
      {
        return false;
      }
    }

    return true;
  }

  /**
   * Static method to build ServerInfo object from hostPort info.
   *
   * @param hostPort String containing host and port of the server
   * @param hostPortDelim Delimiter between host and port
   * @return ServerInfo
   * @throws Exception
   */
  public static ServerInfo buildServerInfoFromHostPort(String serverHostPort, String hostPortDelim)
    throws Exception
  {
      ServerInfo serverInfo  = null;
      try
      {
        if ( null != serverHostPort)
        {
            String[] hostInfo = serverHostPort.split(hostPortDelim);

            if (hostInfo.length == 2)
            {
                InetSocketAddress address = new InetSocketAddress(InetAddress.getByName(hostInfo[0]), Integer.parseInt(hostInfo[1]));
                serverInfo = new ServerInfo(serverHostPort, DatabusServerCoordinates.StateId.ONLINE.toString(), address);
            }
        }
      } catch(Exception ex) {
        LOG.error("Unable to extract Boostrap Server info from StartSCN response. ServerInfo was :" + serverHostPort, ex);
        throw ex;
      }
      return serverInfo;
  }

  /**
   *
   * @author pganti
   *
   */
  public static class ServerInfoBuilder implements ConfigBuilder<ServerInfo>
  {
    public static final char NAME_SEPARATOR = ')';
    public static final char PORT_SEPARATOR = ':';
    public static final char SOURCES_LIST_SEPARATOR = ':';
    public static final char SOURCE_SEPARATOR = ',';

    private String _host    = "localhost";
    private int    _port    = 9000;
    private String _sources = "";
    private String _name    = null;
    private String _address = null;
    private String _PhysicalSourceName = "DefaultPhysicalSource";
    private SubscriptionUriCodec _uriCodec = LegacySubscriptionUriCodec.getInstance();
    private final List<DatabusSubscription.Builder> _subs =
        new ArrayList<DatabusSubscription.Builder>();

    public ServerInfoBuilder()
    {
    }

    public static String generateServerName(String prefix, int id)
    {
      StringBuilder resBuilder = new StringBuilder();
      resBuilder.append(prefix);
      resBuilder.append('.');
      resBuilder.append(id);
      return resBuilder.toString();
    }


    /**
    * The name of the physical source that is hosted by the relay
    */
    public String getPhysicalSourceName() {
      return _PhysicalSourceName;
    }

    public void setPhysicalSourceName(String PhysicalSourceName) {
      _PhysicalSourceName = PhysicalSourceName;
    }

      /** Host name or IP address of the server */
    public String getHost()
    {
      return _host;
    }

    public void setHost(String host)
    {
      _host = host;
    }

    /** Comma-separated list of sources supported by the server */
    public String getSources()
    {
      return _sources;
    }

    public void setSources(String sources)
    {
      _sources = sources;
    }

    /** The HTTP port on which the server listens */
    public int getPort()
    {
      return _port;
    }

    public void setPort(int port)
    {
      _port = port;
    }

    /** A name that identifies the server */
    public String getName()
    {
      return _name;
    }

    public void setName(String name)
    {
      _name = name;
    }

    public String getAddress()
    {
      return _address;
    }

    /** Format is: [name)]host:port:source1,source2,... */
    public void setAddress(String address)
    {
      _address = address;
    }

    @Override
    public ServerInfo build() throws InvalidConfigException
    {
      if (null != _address) parseAddress();

      String[] sources = getSources().split("[" + SOURCE_SEPARATOR + "]");

      for (int i = 0; i < sources.length; ++i)
        sources[i] = sources[i].trim();

      InetAddress serverAddr = null;
      try
      {
        serverAddr = InetAddress.getByName(getHost());
      }
      catch (Exception e)
      {
        throw new InvalidConfigException("Invalid server address", e);
      }

      InetSocketAddress inetAddress = new InetSocketAddress(serverAddr, getPort());

      if (null == _name)
      {
        StringBuilder serverName = new StringBuilder();
        try
        {
          serverName.append(serverAddr.getHostName());
        }
        catch (Exception e)
        {
          LOG.warn("Unable to resolve address:" + serverAddr.toString());
          serverName.append("server");
        }

        _name = serverName.toString();
      }
      LOG.info("res name: " + _name);
      ServerInfo  serverInfo = new ServerInfo(_name, StateId.ONLINE.toString(), inetAddress, sources);
      serverInfo.setPhysicalSourceName(getPhysicalSourceName());
      return serverInfo;
    }

    public static String generateAddress(String name, String host, int port, String... sources)
    {
      StringBuilder res = new StringBuilder((null != name ? name.length() : 0) + host.length() + 8 + sources.length * 50);
      if (null != name && name.length() > 0)
      {
        res.append(name);
        res.append(NAME_SEPARATOR);
      }
      res.append(host);
      res.append(PORT_SEPARATOR);
      res.append(port);
      res.append(SOURCES_LIST_SEPARATOR);
      boolean first = true;
      for (String s: sources)
      {
        if (!first) res.append(SOURCE_SEPARATOR);
        first = false;
        res.append(s);
      }
      return res.toString();
    }

    void parseAddress() throws InvalidConfigException
    {
      int nameIdx = _address.indexOf(NAME_SEPARATOR);
      if (0 < nameIdx) setName(_address.substring(0, nameIdx));

      int portIdx = _address.indexOf(PORT_SEPARATOR, nameIdx + 1);
      if (0 > portIdx) throw new InvalidConfigException("no port specified in address:" + _address);
      setHost(_address.substring(nameIdx + 1, portIdx));

      int sourceListIdx = _address.indexOf(SOURCES_LIST_SEPARATOR, portIdx + 1);
      if (0 > sourceListIdx) throw new InvalidConfigException("no sources list specified in address:" + _address);
      setPort(Integer.parseInt(_address.substring(portIdx + 1, sourceListIdx)));
      setSources(_address.substring(sourceListIdx + 1));
    }

    public void uriCodec(SubscriptionUriCodec uriCodec)
    {
      _uriCodec = uriCodec;
    }

    public SubscriptionUriCodec uriCodec()
    {
      return _uriCodec;
    }

    public DatabusSubscription.Builder getSub(int index)
    {
      ensureSubsListIndex(index);
      return _subs.get(index);
    }

    private void ensureSubsListIndex(int targetIndex)
    {
      for (int i = _subs.size(); i <= targetIndex; ++i)
      {
        _subs.add(new DatabusSubscription.Builder());
      }
    }
  }

  public static class ServerInfoSetBuilder implements ConfigBuilder<List<ServerInfo>>
  {
    public static final char SERVER_INFO_SEPARATOR = ';';

    private String _servers;

    @Override
    public List<ServerInfo> build() throws InvalidConfigException
    {
      if (null == _servers) return Collections.<ServerInfo>emptyList();
      String[] serverInfos = _servers.split("[" + SERVER_INFO_SEPARATOR + "]");
      ServerInfoBuilder siBuilder = new ServerInfoBuilder();

      ArrayList<ServerInfo> result  = new ArrayList<ServerInfo>(serverInfos.length);
      for (String s: serverInfos)
      {
        siBuilder.setAddress(s);
        result.add(siBuilder.build());
      }

      return result;
    }

    public String getServers()
    {
      return _servers;
    }

    public void setServers(String servers)
    {
      _servers = servers;
    }

  }

  @Override
  public boolean equals(Object obj)
  {
    if (null == obj)
      return false;
    if (!(obj instanceof ServerInfo))
      return false;
    ServerInfo other = (ServerInfo) obj;
    return getAddress().equals(other.getAddress());
  }

  @Override
  public int hashCode()
  {
    return getAddress().hashCode();
  }

@Override
public int compareTo(ServerInfo o)
{
    return _serverCoordinates.compareTo(o._serverCoordinates);
}

}
