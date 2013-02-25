package com.linkedin.databus.client.pub;
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


import java.io.IOException;
import java.util.List;

import org.apache.log4j.Logger;
import org.codehaus.jackson.JsonParseException;
import org.codehaus.jackson.map.JsonMappingException;

import com.linkedin.databus.core.Checkpoint;
import com.linkedin.databus.core.util.ConfigBuilder;
import com.linkedin.databus.core.util.InvalidConfigException;
import com.linkedin.helix.AccessOption;
import com.linkedin.helix.HelixException;
import com.linkedin.helix.HelixManager;
import com.linkedin.helix.HelixManagerFactory;
import com.linkedin.helix.InstanceType;
import com.linkedin.helix.ZNRecord;
import com.linkedin.helix.manager.zk.ZKHelixAdmin;
import com.linkedin.helix.manager.zk.ZNRecordSerializer;
import com.linkedin.helix.manager.zk.ZkClient;
import com.linkedin.helix.store.HelixPropertyStore;

public class ClusterCheckpointPersistenceProvider extends
        CheckpointPersistenceProviderAbstract
{

    protected static final Logger LOG = Logger.getLogger(ClusterCheckpointPersistenceProvider.class);

    private final String _id;
    private HelixPropertyStore<ZNRecord> _propertyStore = null;

    
    public ClusterCheckpointPersistenceProvider(long id) throws InvalidConfigException 
    {
        this(id,new Config());
    }
    
    public ClusterCheckpointPersistenceProvider(String id) throws InvalidConfigException 
    {
        this(id,new Config().build());
    }
    
    public ClusterCheckpointPersistenceProvider(long id,Config config) throws InvalidConfigException
    {
        this(id,config.build());
    }
    
    public ClusterCheckpointPersistenceProvider(String id,Config config) throws InvalidConfigException
    {
        this(id,config.build());
    }
    
    public ClusterCheckpointPersistenceProvider(long id,StaticConfig config) throws InvalidConfigException
    {
        this("" + id,config);
    }
    
    public ClusterCheckpointPersistenceProvider(String id,StaticConfig config)
    {
        _id = id;
        try
        { 
            ZkClient zkClient = new ZkClient(config.getZkAddr(), ZkClient.DEFAULT_SESSION_TIMEOUT,
                ZkClient.DEFAULT_CONNECTION_TIMEOUT, new ZNRecordSerializer());
            ZKHelixAdmin admin = new ZKHelixAdmin(zkClient);
            try 
            {
                admin.addCluster(config.getClusterName(), false);
            }
            catch (HelixException e)
            {
                LOG.warn("Warn! Cluster might already exist! " + config.getClusterName());
            }
            HelixManager manager = HelixManagerFactory.getZKHelixManager(config.getClusterName(),
                    _id,
                    InstanceType.SPECTATOR,
                    config.getZkAddr());
            manager.connect();
            _propertyStore = manager.getHelixPropertyStore();
        }
        catch (Exception e)
        {
            LOG.error("Error creating Helix Manager! for cluster=" + config.getClusterName() +  " id=" + _id + " exception=" +  e );
        }
    }
    
    /**
     * 
     * @param srcs
     * @return concatenated sources
     */
    protected String makeKey(List<String> srcs)
    {
        StringBuilder k = new StringBuilder(50);
        k.append(_id);
        for (String s: srcs)
        {
            k.append("_");
            k.append(s);
        }
        return k.toString();
    }
    
    @Override
    public void storeCheckpoint(List<String> sourceNames, Checkpoint checkpoint) throws IOException
    {
       if (_propertyStore != null)
       {
           String key = makeKey(sourceNames);
           ZNRecord znRecord = new ZNRecord(_id);
           znRecord.setSimpleField("c",checkpoint.toString());
           _propertyStore.set(key,znRecord,AccessOption.PERSISTENT);
       }
    }

    @Override
    public Checkpoint loadCheckpoint(List<String> sourceNames)
    {
        if (_propertyStore != null)
        {
            String key = makeKey(sourceNames);

            ZNRecord zn = _propertyStore.get(key, null, AccessOption.PERSISTENT);
            if (zn != null)
            {
                String v =  zn.getSimpleField("c");
                try 
                {
                    Checkpoint cp;
                    cp = new Checkpoint(v);
                    return cp;
                }
                catch (JsonParseException e)
                {
                    LOG.error("Cannot deserialize value for key=" + key +  " value=" + v + " exception="+ e);
                }
                catch (JsonMappingException e)
                {
                    LOG.error("Cannot deserialize value for key=" + key +  " value=" + v + " exception="+ e);
                }
                catch (IOException e)
                {
                    LOG.error("Cannot deserialize value for key=" + key +  " value=" + v + " exception="+ e);
                }
            }
            else
            {
                LOG.error("No record for key = " + key);
            }
        }
        return null;
    }

    @Override
    public void removeCheckpoint(List<String> sourceNames)
    {
        if (_propertyStore != null)
        {
            
        }
    }
    
    public static class StaticConfig
    {
        private final int _maxNumWritesSkipped;
        private final String _zkAddr ; 
        private final String _clusterName;

        public StaticConfig(String zkAddr, String clusterName,int maxNumWritesSkipped)
        {
            _zkAddr = zkAddr;
            _clusterName = clusterName;
            _maxNumWritesSkipped = maxNumWritesSkipped;
        }

        public String getZkAddr()
        {
            // TODO Auto-generated method stub
            return _zkAddr;
        }

        public String getClusterName()
        {
            // TODO Auto-generated method stub
            return _clusterName;
        }

        public int getMaxNumWritesSkipped()
        {
            return _maxNumWritesSkipped;
        }
    }

    public static class Config implements ConfigBuilder<StaticConfig>
    {
        private String _zkAddr = null;
        private String _clusterName = null; 
        private  int _maxNumWritesSkipped=0;
        
        public Config()
        {
        }

        @Override
        public StaticConfig build() throws InvalidConfigException
        {
            if (_zkAddr==null || _clusterName==null)
            {
              throw new InvalidConfigException ("zkAddr or clusterName cannot be unspecified ");
            }
            return new StaticConfig(_zkAddr,_clusterName,_maxNumWritesSkipped);
        }

        public int getMaxNumWritesSkipped()
        {
            return _maxNumWritesSkipped;
        }

        public void setMaxNumWritesSkipped(int frequencyOfWritesInEvents)
        {
            _maxNumWritesSkipped = frequencyOfWritesInEvents;
        }

        public String getZkAddr()
        {
            return _zkAddr;
        }

        public void setZkAddr(String zkAddr)
        {
            _zkAddr = zkAddr;
        }

        public String getClusterName()
        {
            return _clusterName;
        }

        public void setClusterName(String clusterName)
        {
            _clusterName = clusterName;
        }

    }

    
    

}
