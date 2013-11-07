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
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;
import org.codehaus.jackson.JsonParseException;
import org.codehaus.jackson.map.JsonMappingException;

import com.linkedin.databus.core.Checkpoint;
import com.linkedin.databus.core.util.ConfigBuilder;
import com.linkedin.databus.core.util.InvalidConfigException;
import org.apache.helix.AccessOption;
import org.apache.helix.HelixException;
import org.apache.helix.HelixManager;
import org.apache.helix.HelixManagerFactory;
import org.apache.helix.InstanceType;
import org.apache.helix.ZNRecord;
import org.apache.helix.manager.zk.ZKHelixAdmin;
import org.apache.helix.manager.zk.ZNRecordSerializer;
import org.apache.helix.manager.zk.ZkClient;
import org.apache.helix.store.HelixPropertyStore;

public class ClusterCheckpointPersistenceProvider extends
        CheckpointPersistenceProviderAbstract
{

    protected static final Logger LOG = Logger
            .getLogger(ClusterCheckpointPersistenceProvider.class);
    
    //Internal key-name where the value of 'checkpoint' will be stored
    protected static final String KEY_CHECKPOINT="c";
    //Internal key-name where the value of 'sources' will be stored
    protected static final String KEY_SOURCES="s";

    private final String _id;
    private HelixPropertyStore<ZNRecord> _propertyStore = null;
    private final long _checkpointIntervalMs;
    private long _numWritesSkipped = 0;
    private long _lastTimeWrittenMs = 0;

    private final static HelixConnectionManager _helixConnManager = new HelixConnectionManager();

    public ClusterCheckpointPersistenceProvider(long id)
            throws InvalidConfigException, ClusterCheckpointException
    {
        this(id, new Config());
    }

    public ClusterCheckpointPersistenceProvider(String id)
            throws InvalidConfigException, ClusterCheckpointException
    {
        this(id, new Config().build());
    }

    public ClusterCheckpointPersistenceProvider(long id, Config config)
            throws InvalidConfigException, ClusterCheckpointException
    {
        this(id, config.build());
    }

    public ClusterCheckpointPersistenceProvider(String id, Config config)
            throws InvalidConfigException, ClusterCheckpointException
    {
        this(id, config.build());
    }

    public ClusterCheckpointPersistenceProvider(long id, StaticConfig config)
            throws InvalidConfigException, ClusterCheckpointException
    {
        this(Long.toString(id), config);
    }

    /**
     * Create checkpoint persistence object for a partition.
     * 
     * @param id
     *            : partition id
     * @param config
     *            : specifies zookeeper server ,cluster name, minimum frequency
     *            of writing checkpoints in ms
     * @throws ClusterCheckpointException
     *             on errors like absence of a cluster; use static method
     *             createCluster to create a cluster
     */
    public ClusterCheckpointPersistenceProvider(String id, StaticConfig config)
            throws ClusterCheckpointException
    {
        _id = id;
        _checkpointIntervalMs = config.getCheckpointIntervalMs();
        try
        {
            HelixManager manager = _helixConnManager.open(
                    config.getClusterName(), config.getZkAddr(), id);
            _propertyStore = manager.getHelixPropertyStore();
        }
        catch (Exception e)
        {
            LOG.error("Error creating Helix Manager! for cluster="
                    + config.getClusterName() + " id=" + _id + " exception="
                    + e);
            throw new ClusterCheckpointException(e.toString());
        }
    }

    /**
     * Create a cluster if it doesn't exist Note: This method is not thread-safe
     * as HelixAdmin.addCluster appears to fail on concurrent execution within
     * threads
     * 
     * @return true if cluster was created false otherwise
     */
    static public boolean createCluster(String zkAddr, String clusterName)
    {
        boolean created = false;
        ZkClient zkClient = null;
        try
        {
             zkClient = new ZkClient(zkAddr,
                    ZkClient.DEFAULT_SESSION_TIMEOUT,
                    ZkClient.DEFAULT_CONNECTION_TIMEOUT, new ZNRecordSerializer());
            ZKHelixAdmin admin = new ZKHelixAdmin(zkClient);
            admin.addCluster(clusterName, false);
            created=true;
        }
        catch (HelixException e)
        {
            LOG.warn("Warn! Cluster might already exist! " + clusterName);
            created=false;
        }
        finally
        {
            // close this connection
            if (zkClient != null)
            {
                zkClient.close();
            }
        }
        return created;
    }

    /**
     * Call close() when the cluster is shutdown to explicitly tear down
     * connections to corresponding helix manager
     */
    static public void close(String clusterName)
    {
        if (_helixConnManager != null)
        {
            _helixConnManager.close(clusterName);
        }
    }

    /**
     * new format of key; prepended "/" . construction with truncated source if possible e.g.
     * e.g. input=srcs=com.linkedin.events.conns.Connections,com.linkedin.events.conns.ConnectionsCnt and _id=5
     * output: /5_Connections_ConnectionsCnt
     * 
     * @param srcs
     * @return flat String source separated by _
     */
    protected String makeKey(List<String> srcs)
    {
        StringBuilder k = new StringBuilder(50);
        k.append("/");
        k.append(_id);
        for (String s : srcs)
        {
            k.append("_");
            String[] list = s.split("\\.");
            k.append(list[list.length - 1]);
        }
        return k.toString();
    }

    /**
     * Deprecated
     * old style key construction without '/' prepended
     * 
     * @param srcs
     * @return flat String source separated by _
     */
    protected String makeKeyOld(List<String> srcs)
    {
        StringBuilder k = new StringBuilder(50);
        k.append(_id);
        for (String s : srcs)
        {
            k.append("_");
            k.append(s);
        }
        return k.toString();
    }

    @Deprecated
    /**
     * 
     * @param sourceNames
     * @param checkpoint
     *            Persist a checkpoint in legacy location
     * @throws IOException
     */
    public void storeCheckpointLegacy(List<String> sourceNames,
            Checkpoint checkpoint) throws IOException
    {
        if (_propertyStore != null)
        {
            long curtimeMs = System.currentTimeMillis();
            if ((curtimeMs - _lastTimeWrittenMs) > _checkpointIntervalMs)
            {
                String key = makeKeyOld(sourceNames);
                ZNRecord znRecord = new ZNRecord(_id);
                znRecord.setSimpleField(KEY_CHECKPOINT, checkpoint.toString());
                _propertyStore.set(key, znRecord, AccessOption.PERSISTENT);
                _lastTimeWrittenMs = curtimeMs;
                _numWritesSkipped = 0;
            }
            else
            {
                _numWritesSkipped++;
            }
        }
    }

    /**
     * Called by databus client library to persist checkpoint
     */
    @Override
    public void storeCheckpoint(List<String> sourceNames, Checkpoint checkpoint)
            throws IOException
    {
        if (_propertyStore != null)
        {
            long curtimeMs = System.currentTimeMillis();
            if ((curtimeMs - _lastTimeWrittenMs) > _checkpointIntervalMs)
            {
                String key = makeKey(sourceNames);
                ZNRecord znRecord = new ZNRecord(_id);
                znRecord.setSimpleField(KEY_CHECKPOINT, checkpoint.toString());
                znRecord.setSimpleField(KEY_SOURCES,
                        StringUtils.join(sourceNames.toArray(), ","));
                _propertyStore.set(key, znRecord, AccessOption.PERSISTENT);
                _lastTimeWrittenMs = curtimeMs;
                _numWritesSkipped = 0;
            }
            else
            {
                _numWritesSkipped++;
            }
        }
    }

    @Deprecated
    /**
     * read legacy checkpoint without migration
     * 
     * @param sources
     * @return checkpoint if found or null otherwise
     */
    public Checkpoint loadCheckpointLegacy(List<String> sources)
    {
        String key = makeKeyOld(sources);
        Checkpoint cp = getCheckpoint(key);
        return cp;
    }

    /**
     * internal function that fetches contents from Helix property store
     * 
     * @param key
     * @return checkpoint or null
     */
    private Checkpoint getCheckpoint(String key)
    {
        ZNRecord zn = _propertyStore.get(key, null, AccessOption.PERSISTENT);
        if (zn != null)
        {
            String v = zn.getSimpleField(KEY_CHECKPOINT);
            try
            {
                Checkpoint cp;
                cp = new Checkpoint(v);
                return cp;
            }
            catch (JsonParseException e)
            {
                LOG.error("Cannot deserialize value for key=" + key + " value="
                        + v + " exception=" + e);
            }
            catch (JsonMappingException e)
            {
                LOG.error("Cannot deserialize value for key=" + key + " value="
                        + v + " exception=" + e);
            }
            catch (IOException e)
            {
                LOG.error("Cannot deserialize value for key=" + key + " value="
                        + v + " exception=" + e);
            }
        }
        else
        {
            LOG.error("No record for key = " + key);
        }
        return null;
    }

    /**
     * Function called by Databus Client to load checkpoint;
     */
    @Override
    public Checkpoint loadCheckpoint(List<String> sourceNames)
    {
        if (_propertyStore != null)
        {
            String key = makeKey(sourceNames);
            Checkpoint cp = getCheckpoint(key);
            return cp;
        }
        return null;
    }

    /**
     * Find unique sourceNames of checkpoints across all partitions found in the
     * cluster
     * 
     * @return set of unique sources
     */

    public Set<String> getSourceNames()
    {
        if (_propertyStore != null)
        {
            // note that "/" is the root - it's prepended in 'makeKey'
            List<String> keys = _propertyStore.getChildNames("/",
                    AccessOption.PERSISTENT);
            if (keys != null)
            {
                HashSet<String> sources = new HashSet<String>();
                for (String k : keys)
                {
                    // add the "/" again
                    ZNRecord zn = _propertyStore.get("/" + k, null,
                            AccessOption.PERSISTENT);
                    if (zn != null)
                    {
                        String srcName = zn.getSimpleField(KEY_SOURCES);
                        if (srcName != null)
                        {
                            sources.add(srcName);
                        }
                    }
                }
                return sources;
            }
        }
        return null;
    }

    private void removeCheckpoint(String key)
    {
        _propertyStore.remove(key, AccessOption.PERSISTENT);
    }

    public void removeCheckpointLegacy(List<String> sourceNames)
    {
        String keyOld = makeKeyOld(sourceNames);
        removeCheckpoint(keyOld);
    }

    @Override
    public void removeCheckpoint(List<String> sourceNames)
    {
        if (_propertyStore != null)
        {
            String key = makeKey(sourceNames);
            removeCheckpoint(key);
        }
    }

    public long getNumWritesSkipped()
    {
        return _numWritesSkipped;
    }

    @SuppressWarnings("serial")
    public static class ClusterCheckpointException extends Exception
    {
        public ClusterCheckpointException(String msg)
        {
            super(msg);
        }
    }

    /**
     * 
     * Reuse connections to helix manager across partitions;
     * one per cluster
     */
    private static class HelixConnectionManager
    {
        final private Map<String, HelixManager> _managers;

        public HelixConnectionManager()
        {
            _managers = new HashMap<String, HelixManager>();
        }

        /**
         * given a cluster; return a helix connection if one exists; otherwise
         * create one
         * 
         * @param cluster
         */
        public synchronized HelixManager open(String clusterName,
                String zkAddr, String id) throws Exception
        {
            HelixManager m = _managers.get(clusterName);
            if (null == m)
            {
                m = HelixManagerFactory.getZKHelixManager(clusterName, id,
                        InstanceType.SPECTATOR, zkAddr);
                _managers.put(clusterName, m);
                if (!m.isConnected())
                {
                    m.connect();
                }
            }
            return m;
        }

        /**
         * shutdown managers explicitly
         */
        public synchronized void close(String cluster)
        {
            HelixManager m = _managers.get(cluster);
            if (m != null)
            {
                _managers.remove(cluster);
                m.disconnect();
            }
        }

    }

    public static class StaticConfig
    {
        private final String _zkAddr;
        private final String _clusterName;
        private final long _checkpointIntervalMs;
        private final int _maxNumWritesSkipped;

        public StaticConfig(String zkAddr, String clusterName,
                int numWritesSkipped, long checkpointIntervalMs)
        {
            _zkAddr = zkAddr;
            _clusterName = clusterName;
            _maxNumWritesSkipped = numWritesSkipped;
            _checkpointIntervalMs = checkpointIntervalMs;
        }

        public String getZkAddr()
        {
            return _zkAddr;
        }

        public String getClusterName()
        {
            return _clusterName;
        }

        @Deprecated
        public int getMaxNumWritesSkipped()
        {
            return _maxNumWritesSkipped;
        }

        public long getCheckpointIntervalMs()
        {
            return _checkpointIntervalMs;
        }
    }

    public static class Config implements ConfigBuilder<StaticConfig>
    {
        private String _zkAddr = null;
        private String _clusterName = null;
        private long _checkpointIntervalMs = 5 * 60 * 1000; // 5 minutes

        /** Deprecated - conf setting has no effect **/
        private int _maxNumWritesSkipped = 0;

        public Config()
        {
        }

        @Override
        public StaticConfig build() throws InvalidConfigException
        {
            if (_zkAddr == null || _clusterName == null)
            {
                throw new InvalidConfigException(
                        "zkAddr or clusterName cannot be unspecified ");
            }
            return new StaticConfig(_zkAddr, _clusterName,
                    _maxNumWritesSkipped, _checkpointIntervalMs);
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

        @Deprecated
        /** Deprecated - conf setting has no effect **/
        public int getMaxNumWritesSkipped()
        {
            return _maxNumWritesSkipped;
        }

        @Deprecated
        /** Deprecated - conf setting has no effect **/
        public void setMaxNumWritesSkipped(int maxNumWritesSkipped)
        {
            _maxNumWritesSkipped = maxNumWritesSkipped;
        }

        public void setCheckpointIntervalMs(long checkpointIntervalMs)
        {
            _checkpointIntervalMs = checkpointIntervalMs;
        }

        public long getCheckpointIntervalMs()
        {
            return _checkpointIntervalMs;
        }

    }

}
