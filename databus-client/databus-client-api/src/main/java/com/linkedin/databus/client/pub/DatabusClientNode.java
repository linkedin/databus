package com.linkedin.databus.client.pub;

import java.util.HashMap;
import java.util.Random;

import org.apache.log4j.Logger;

import com.linkedin.databus.core.util.ConfigBuilder;
import com.linkedin.databus.core.util.InvalidConfigException;
import com.linkedin.databus.groupleader.impl.zkclient.GroupLeadershipConnectionFactoryZkClientImpl;
import com.linkedin.databus.groupleader.pub.GroupLeadershipConnection;
import com.linkedin.databus.groupleader.pub.GroupLeadershipConnectionFactory;
/**
 *
 * @author snagaraj
 *Abstraction for cluster node; each of these connect to a zk server; it is possible to have many "members" share the connection;
 *This is not thread safe. It is meant to be used within a thread; the moment the thread dies; the shared state - in this case zk will delete all 'ephemeral' data created
 *by the connection and its members. Note that the connection itself is physical, the sharing of data, group membership is handled at the domain and group level.
 */
public class DatabusClientNode
{
  public static final String MODULE = DatabusClientNode.class.getName();
  public static final Logger LOG = Logger.getLogger(MODULE);

  //represents a member belonging to a certain domain ; group ;
  private HashMap<String,DatabusClientGroupMember> _members;
  //represents the 'physical' node connection to group;
  private GroupLeadershipConnection _connection = null;
  // config
  private final StaticConfig _config;


  public DatabusClientNode(StaticConfig config) throws InvalidConfigException
  {
    _config = config;
    if (config.isEnabled())
    {
      if (config.getClusterServerList().isEmpty())
      {
        throw new InvalidConfigException("Property val is empty! clusterServerList ");
      }
      _members = new HashMap<String,DatabusClientGroupMember> ();
      open();

    }

  }

  //used only for testing;
  void  open()
  {
	if (isEnabled())
	{
		GroupLeadershipConnectionFactory groupLeadershipConnFactory = new GroupLeadershipConnectionFactoryZkClientImpl(_config.getClusterServerList(),
				_config.getSessionTimeoutMillis(),
				_config.getConnectionTimeoutMillis());

		_connection = groupLeadershipConnFactory.getConnection();
		addMember(_config.getDomain(),_config.getGroup(),_config.getName(),_config.getSharedDataPath(),_config.isMastershipRequiredForWrite());
	}
  }

  public DatabusClientNode(Config config) throws InvalidConfigException
  {
    this(config.build());
  }

  /** Is cluster mode enabled */
  public boolean isEnabled()
  {
    return _config.isEnabled();
  }

  /** Is the node connected to the set of cluster machines */
  public boolean isConnected()
  {
    return _connection != null;
  }

  public StaticConfig getStaticConfig()
  {
    return _config;
  }

  public HashMap<String,DatabusClientGroupMember>  getMembers()
  {
      return _members;
  }

  /**
   * retrieve the DatbusClientGroupMember from the connection ; whose domain, group and name matches the input
   * @return the DatabusClientGroupMember if found,  null otherwise;
   */
  public DatabusClientGroupMember getMember(String domain,String group,String name)
  {
    if (isConnected())
    {
      String key = makeKey(domain,group,name);
      return _members.get(key);
    }
    return null;
  }
  /**
   * create a DatabusClientGroupMember object given namespace, group and name
   * This does not attempt to join the group ; invoke join on the member object;
   * @return the DatabusClientGroupMember if successful, null if either the member already existed or if the client isn't connected to cluster ;
   */
   public DatabusClientGroupMember addMember(String domain,String group,String name,String sharedDataPath,boolean requiredMastership)
   {
     if (isConnected())
     {
       String key = makeKey(domain,group,name);
       if (!_members.containsKey(key)) {
         DatabusClientGroupMember member = new DatabusClientGroupMember(domain,group,name,sharedDataPath, _connection,null,requiredMastership);
         _members.put(key,member);
         return member;
       } else {
         LOG.info("Error! Inserting duplicate member with key: " + key);
       }
     }
     return null;
   }

   /**
    * find a member and execute member.leave(); it is possible for the same member to be created at a later point using addMember()
    * @return true if a member was removed ;
    */
   public boolean removeMember(String domain, String group,String name)
   {
     if (isConnected())
     {
         String key = makeKey(domain,group,name);
         DatabusClientGroupMember groupMember = this.getMember(domain,group,name);
         if (groupMember != null)
         {
           groupMember.leave();
           _members.remove(key);
           return true;
         }
     }
     return false;
   }

   private String makeKey(String domain,String group,String name)
   {
       return domain + "_" + group + "_" + name;
   }

   public void join()
   {
	   if (_connection != null)
	   {
		   for (DatabusClientGroupMember member: _members.values())
		   {
			   member.join();
		   }
	   }
   }

   public void leave()
   {
	   if (_connection != null)
	   {
		   for (DatabusClientGroupMember member: _members.values())
		   {
			   member.leave();
		   }
	   }
   }

   public void close()
   {
     if (_connection!=null)
     {
    	 //members have to leave() explicitly
       _members.clear();
       _connection.close();
     }
   }


   /** Static configuration for the DatabusClientNode.
    *
    * @see DatabusClientNode
    */
   public static class StaticConfig
   {
     private final boolean _enabled;
     private final String _clusterServerList;
     private final int _sessionTimeoutMillis;
     private final int _connectionTimeoutMillis;
     private final String _group;
     private final String _domain;
     private final String _name;
     private final boolean _mastershipRequiredForWrite;
     private final String _sharedDataPath;


     public StaticConfig(boolean enabled, String clusterServerList,int sessionTimeoutMillis, int connectionTimeoutMillis,String domain,String group,
                         String name,boolean requiresMastership,String sharedDataPath)
     {
       _enabled = enabled;
       _clusterServerList = clusterServerList;
       _sessionTimeoutMillis = sessionTimeoutMillis ;
       _connectionTimeoutMillis = connectionTimeoutMillis;
       _domain = domain;
       _name = name;
       _group = group;
       _mastershipRequiredForWrite = requiresMastership;
       _sharedDataPath = sharedDataPath;
     }

    public String getSharedDataPath()
    {
      return _sharedDataPath;
    }

    public String getClusterServerList()
    {
      return _clusterServerList;
    }

    public int getSessionTimeoutMillis()
    {
      return _sessionTimeoutMillis;
    }

    public int getConnectionTimeoutMillis()
    {
      return _connectionTimeoutMillis;
    }

    public String getGroup()
    {
      return _group;
    }

    public String getDomain()
    {
      return _domain;
    }

    public String getName()
    {
      return _name;
    }

    public boolean isMastershipRequiredForWrite()
    {
      return _mastershipRequiredForWrite ;
    }

    public boolean isEnabled()
    {
      return _enabled;
    }

   }

   public static class Config implements ConfigBuilder<StaticConfig>
   {
     private String _clusterServerList="";
     private int _sessionTimeoutMillis=5000;
     private int _connectionTimeoutMillis=10000;
     private String _group="default-databus-group";
     private String _domain="/Databus-Client-Domain";
     private String _name="default-name";
     private boolean _enabled = false;
     private boolean _mastershipRequiredForWrite = true;
     private String _sharedDataPath = null;

     public Config()
     {
       super();
       Random r = new Random(System.currentTimeMillis());
       _name = _name + r.nextInt();
     }

     public String getClusterServerList()
    {
      return _clusterServerList;
    }



    public int getSessionTimeoutMillis()
    {
      return _sessionTimeoutMillis;
    }



    public int getConnectionTimeoutMillis()
    {
      return _connectionTimeoutMillis;
    }



    public void setClusterServerList(String clusterServerList)
    {
      _clusterServerList = clusterServerList;
    }



    public void setSessionTimeoutMillis(int sessionTimeoutMillis)
    {
      _sessionTimeoutMillis = sessionTimeoutMillis;
    }



    public void setConnectionTimeoutMillis(int connectionTimeoutMillis)
    {
      _connectionTimeoutMillis = connectionTimeoutMillis;
    }


    @Override
     public StaticConfig build() throws InvalidConfigException
     {
       return new StaticConfig(_enabled, _clusterServerList,_sessionTimeoutMillis,_connectionTimeoutMillis,_domain,_group,_name,_mastershipRequiredForWrite,_sharedDataPath );
     }

    public String getGroup()
    {
      return _group;
    }

    public String getDomain()
    {
      return _domain;
    }

    public String getName()
    {
      return _name;
    }

    public void setGroup(String group)
    {
      _group = group;
    }

    public void setDomain(String domain)
    {
      _domain = domain;
    }

    public void setName(String name)
    {
      _name = name;
    }

    public boolean isEnabled()
    {
      return _enabled;
    }

    public void setEnabled(boolean enabled)
    {
      _enabled = enabled;
    }

    public boolean isMastershipRequiredForWrite()
    {
      return _mastershipRequiredForWrite;
    }

    public void setMastershipRequiredForWrite(boolean requireMastershipForWrite)
    {
      _mastershipRequiredForWrite = requireMastershipForWrite;
    }

    public String getSharedDataPath()
    {
      return _sharedDataPath;
    }

    public void setSharedDataPath(String sharedDataPath)
    {
      _sharedDataPath = sharedDataPath;
    }

   }


}
