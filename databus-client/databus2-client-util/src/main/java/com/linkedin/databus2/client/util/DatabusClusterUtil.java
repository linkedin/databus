package com.linkedin.databus2.client.util;

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
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;

import com.linkedin.databus.client.pub.ClusterCheckpointPersistenceProvider;
import com.linkedin.databus.client.pub.ClusterCheckpointPersistenceProvider.ClusterCheckpointException;
import com.linkedin.databus.cluster.DatabusCluster;
import com.linkedin.databus.core.Checkpoint;
import com.linkedin.databus.core.DbusClientMode;
import com.linkedin.databus.core.util.InvalidConfigException;
import org.apache.helix.manager.zk.ZKHelixAdmin;
import org.apache.helix.manager.zk.ZNRecordSerializer;
import org.apache.helix.manager.zk.ZkClient;
import org.apache.helix.model.ExternalView;
import org.apache.helix.model.IdealState;

public class DatabusClusterUtil
{

    static public class DatabusClusterUtilHelper
    {
        protected final ZkClient _zkClient;
        protected final ZKHelixAdmin _admin;
        protected final String _clusterName;
        // list of names of instances/jvm's in a cluster
        List<String> _instances = null;
        private HashMap<Integer, String> _partitionMap = new HashMap<Integer, String>(
                100);

        public DatabusClusterUtilHelper(String zkAddr, String clusterName)
        {
            _clusterName = clusterName;
            _zkClient = new ZkClient(zkAddr, ZkClient.DEFAULT_SESSION_TIMEOUT,
                    ZkClient.DEFAULT_CONNECTION_TIMEOUT,
                    new ZNRecordSerializer());
            _admin = new ZKHelixAdmin(_zkClient);
        }

        public void getClusterInfo()
        {
            List<String> resources = getResources();
            if (resources.size() < 1)
            {
                System.err.println("Error! No resources found in cluster  "
                        + _clusterName);
                return;
            }
            IdealState idealState = _admin.getResourceIdealState(_clusterName,
                    resources.get(0));
            idealState.getNumPartitions();
            ExternalView v = _admin.getResourceExternalView(_clusterName,
                    resources.get(0));
            if (v == null)
            {
                System.err.println("No instances running for cluster= "
                        + _clusterName + " resource= " + resources.get(0));
                return;
            }
            _partitionMap.clear();
            for (String k : v.getPartitionSet())
            {
                Map<String, String> map = v.getStateMap(k);
                if (map != null)
                {
                    for (Map.Entry<String, String> mkPair : map.entrySet())
                    {
                        String value = mkPair.getValue();
                        if (value != null)
                        {
                            Integer partition = getPartition(k);
                            if (value.equals("ONLINE"))
                            {
                                _partitionMap.put(partition, mkPair.getKey());
                            }
                        }
                    }
                }
            }
        }

        public List<String> getResources()
        {
            return _admin.getResourcesInCluster(_clusterName);
        }

        public void createCluster(int numPartitions)
        {
            DatabusCluster.create(_admin, _clusterName, numPartitions);
        }

        public void removeCluster()
        {
            _admin.dropCluster(_clusterName);
        }

        public boolean existsCluster()
        {
            try
            {
                List<String> resources = getResources();
                return (resources != null && !resources.isEmpty());
            }
            catch (Exception e)
            {
                return false;
            }
        }

        public List<String> getInstances()
        {
            _instances = _admin.getInstancesInCluster(_clusterName);
            return _instances;
        }

        public String getInstanceForPartition(Integer partition)
        {
            return _partitionMap.get(partition);
        }

        public int getNumPartitions()
        {
            List<String> resources = getResources();
            if ((resources != null) && !resources.isEmpty())
            {
                IdealState idealState = _admin.getResourceIdealState(
                        _clusterName, resources.get(0));
                if (idealState != null)
                {
                    return idealState.getNumPartitions();
                }
            }
            return -1;
        }

        public Set<Integer> getPartitionList()
        {
            return _partitionMap.keySet();
        }

        private Integer getPartition(String partition)
        {
            String[] ps = partition.split("_");
            if (ps.length >= 2)
            {
                return Integer.parseInt(ps[ps.length - 1]);
            }
            return -1;
        }

    }

    /** SCN helpers **/
    static public class DatabusClusterCkptManager
    {
        final private ClusterCheckpointPersistenceProvider.Config _clusterConfig;
        final private Set<Integer> _partitions;
        final private List<String> _sources;
        final boolean _isLegacyCkptLocation;

        public DatabusClusterCkptManager(String zkAddr, String cluster,
                List<String> sources, Set<Integer> partitions,
                boolean isLegacyCkptLocation)
        {
            _clusterConfig = new ClusterCheckpointPersistenceProvider.Config();
            _clusterConfig.setClusterName(cluster);
            _clusterConfig.setZkAddr(zkAddr);
            _clusterConfig.setCheckpointIntervalMs(1);
            _partitions = partitions;
            _sources = sources;
            _isLegacyCkptLocation = isLegacyCkptLocation;
        }

        public void writeCheckpoint(long scn)
                throws DatabusClusterUtilException
        {
            try
            {
                for (int p : _partitions)
                {
                    ClusterCheckpointPersistenceProvider cpProvider = new ClusterCheckpointPersistenceProvider(
                            p, _clusterConfig);
                    Checkpoint cp = new Checkpoint();
                    cp.setConsumptionMode(DbusClientMode.ONLINE_CONSUMPTION);
                    cp.setWindowOffset(-1);
                    cp.setWindowScn(scn);
                    if (_isLegacyCkptLocation)
                    {
                        cpProvider.storeCheckpointLegacy(_sources, cp);
                    }
                    else
                    {
                        cpProvider.storeCheckpoint(_sources, cp);
                    }
                }
            }
            catch (InvalidConfigException e)
            {
                throw new DatabusClusterUtilException(e.toString());
            }
            catch (IOException e)
            {
                throw new DatabusClusterUtilException(e.toString());
            }
            catch (ClusterCheckpointException e)
            {
                throw new DatabusClusterUtilException(e.toString());
            }
        }

        /**
         * return checkpoints from partitions
         * 
         * @throws DatabusClusterUtilException
         */
        public Map<Integer, Checkpoint> readCheckpoint()
                throws DatabusClusterUtilException
        {
            HashMap<Integer, Checkpoint> list = new HashMap<Integer, Checkpoint>(
                    _partitions.size());
            try
            {
                for (int p : _partitions)
                {
                    ClusterCheckpointPersistenceProvider cpProvider = new ClusterCheckpointPersistenceProvider(
                            p, _clusterConfig);
                    if (_isLegacyCkptLocation)
                    {
                        list.put(p, cpProvider.loadCheckpointLegacy(_sources));
                    }
                    else
                    {
                        list.put(p, cpProvider.loadCheckpoint(_sources));
                    }
                }
            }
            catch (InvalidConfigException e)
            {
                throw new DatabusClusterUtilException(e.toString());
            }
            catch (ClusterCheckpointException e)
            {
                throw new DatabusClusterUtilException(e.toString());
            }
            return list;
        }

        public void remove() throws DatabusClusterUtilException
        {
            try
            {
                for (int p : _partitions)
                {
                    ClusterCheckpointPersistenceProvider cpProvider = new ClusterCheckpointPersistenceProvider(
                            p, _clusterConfig);
                    if (_isLegacyCkptLocation)
                    {
                        cpProvider.removeCheckpointLegacy(_sources);
                    }
                    else
                    {
                        cpProvider.removeCheckpoint(_sources);
                    }
                }
            }
            catch (InvalidConfigException e)
            {
                throw new DatabusClusterUtilException(e.toString());
            }
            catch (ClusterCheckpointException e)
            {
                throw new DatabusClusterUtilException(e.toString());
            }
        }

        public Set<String> getSourcesFromCheckpoint()
                throws DatabusClusterUtilException
        {
            try
            {
                // this only works with new checkpoints
                ClusterCheckpointPersistenceProvider cpProvider = new ClusterCheckpointPersistenceProvider(
                        0, _clusterConfig);
                return cpProvider.getSourceNames();
            }
            catch (InvalidConfigException e)
            {
                throw new DatabusClusterUtilException(e.toString());
            }
            catch (ClusterCheckpointException e)
            {
                throw new DatabusClusterUtilException(e.toString());
            }
        }

    }

    static public Set<Integer> getPartitions(String partition, int numParts)
            throws DatabusClusterUtilException
    {
        // get the list of finalPartitions
        Set<Integer> partList = null;
        if (partition != null && !partition.isEmpty())
        {
            String partitions[] = partition.split(",");
            partList = new HashSet<Integer>(partitions.length);
            for (String pt : partitions)
            {
                Integer part = Integer.parseInt(pt);
                if (part <= numParts)
                {
                    partList.add(part);
                }
                else
                {
                    throw new DatabusClusterUtilException("Partition " + part
                            + " not legal in " + numParts + " partitions");
                }
            }
        }
        else
        {
            partList = new HashSet<Integer>(numParts);
            for (int i = 0; i < numParts; ++i)
            {
                partList.add(i);
            }
        }
        return partList;
    }

    static public class DatabusClusterUtilException extends Exception
    {

        private static final long serialVersionUID = 1L;

        public DatabusClusterUtilException(String msg)
        {
            super(msg);
        }
    }

    /**
     * @param args
     *            DbusClusterUtil -z <zookeper-server> -c <cluster-name> [-p
     *            <partitionNumber] partitions readSCN writeSCN SCN remove
     *            clients
     */
    public static void main(String[] args)
    {
        try
        {
            GnuParser cmdLineParser = new GnuParser();
            Options options = new Options();
            options.addOption("z", true, "zk-server")
                    .addOption("c", true, "cluster-name ")
                    .addOption("p", true, "partition")
                    .addOption("l", false, "legacy")
                    .addOption("h", false, "help");
            CommandLine cmdLineArgs = cmdLineParser.parse(options, args, false);

            if (cmdLineArgs.hasOption('h'))
            {
                usage();
                System.exit(0);
            }

            if (!cmdLineArgs.hasOption('c'))
            {
                usage();
                System.exit(1);
            }
            String clusterName = cmdLineArgs.getOptionValue('c');
            String zkServer = cmdLineArgs.getOptionValue('z');
            boolean isLegacyChkptLocation = cmdLineArgs.hasOption('l');
            if (zkServer == null || zkServer.isEmpty())
            {
                zkServer = "localhost:2181";
            }

            String partitionStr = cmdLineArgs.getOptionValue('p');
            String partition = partitionStr;
            if ((partition != null) && partition.equals("all"))
            {
                partition = "";
            }

            String[] fns = cmdLineArgs.getArgs();
            if (fns.length < 1)
            {
                usage();
                System.exit(1);
            }

            DatabusClusterUtilHelper clusterState = new DatabusClusterUtilHelper(
                    zkServer, clusterName);

            String function = fns[0];
            String arg1 = (fns.length > 1) ? fns[1] : null;
            String arg2 = (fns.length > 2) ? fns[2] : null;

            boolean clusterExists = clusterState.existsCluster();
            if (function.equals("create"))
            {
                if (!clusterExists)
                {
                    if (arg1 == null)
                    {
                        throw new DatabusClusterUtilException(
                                "create: please provide the number of partitions");
                    }
                    int part = Integer.parseInt(arg1);
                    clusterState.createCluster(part);
                    return;
                }
                else
                {
                    throw new DatabusClusterUtilException("Cluster "
                            + clusterName + " already exists");
                }
            }
            if (!clusterExists)
            {
                throw new DatabusClusterUtilException("Cluster doesn't exist! ");
            }

            if (function.equals("delete"))
            {
                clusterState.removeCluster();
            }
            else if (function.equals("partitions"))
            {
                int numParts = clusterState.getNumPartitions();
                System.out.println(numParts);
            }
            else
            {
                // all these functions require the notion of partition;
                Set<Integer> partitions = getPartitions(partition,
                        clusterState.getNumPartitions());
                if (function.equals("sources"))
                {
                    DatabusClusterCkptManager ckptMgr = new DatabusClusterCkptManager(
                            zkServer, clusterName, null, partitions,
                            isLegacyChkptLocation);
                    Set<String> sources = ckptMgr.getSourcesFromCheckpoint();
                    if (sources != null)
                    {
                        for (String s : sources)
                        {
                            System.out.println(s);
                        }
                    } 
                    else
                    {
                        throw new DatabusClusterUtilException("sources: Sources not found for cluster " + clusterName);
                    }
                }
                else if (function.equals("clients"))
                {
                    clusterState.getClusterInfo();
                    for (Integer p : partitions)
                    {
                        String client = clusterState.getInstanceForPartition(p);
                        System.out.println(p + "\t" + client);
                    }
                }
                else if (function.equals("readSCN"))
                {
                    List<String> sources = getSources(arg1);
                    if ((sources != null) && !sources.isEmpty())
                    {
                        DatabusClusterCkptManager ckptMgr = new DatabusClusterCkptManager(
                                zkServer, clusterName, sources, partitions,
                                isLegacyChkptLocation);
                        Map<Integer, Checkpoint> ckpts = ckptMgr
                                .readCheckpoint();
                        char delim = '\t';
                        for (Map.Entry<Integer, Checkpoint> mkPair : ckpts
                                .entrySet())
                        {
                            StringBuilder output = new StringBuilder(64);
                            output.append(mkPair.getKey());
                            output.append(delim);
                            Checkpoint cp = mkPair.getValue();
                            if (cp == null)
                            {
                                output.append(-1);
                                output.append(delim);
                                output.append(-1);
                            }
                            else
                            {
                                if (cp.getConsumptionMode() == DbusClientMode.ONLINE_CONSUMPTION)
                                {
                                    output.append(cp.getWindowScn());
                                    output.append(delim);
                                    output.append(cp.getWindowOffset());
                                }
                                else if (cp.getConsumptionMode() == DbusClientMode.BOOTSTRAP_CATCHUP)
                                {
                                    output.append(cp.getWindowScn());
                                    output.append(delim);
                                    output.append(cp.getWindowOffset());
                                }
                                else if (cp.getConsumptionMode() == DbusClientMode.BOOTSTRAP_SNAPSHOT)
                                {
                                    output.append(cp.getBootstrapSinceScn());
                                    output.append(delim);
                                    output.append(-1);
                                }
                            }
                            System.out.println(output.toString());
                        }
                    }
                    else
                    {
                        throw new DatabusClusterUtilException(
                                "readSCN: please specify non-empty sources");
                    }
                }
                else if (function.equals("checkpoint"))
                {
                    List<String> sources = getSources(arg1);
                    if ((sources != null) && !sources.isEmpty())
                    {
                        DatabusClusterCkptManager ckptMgr = new DatabusClusterCkptManager(
                                zkServer, clusterName, sources, partitions,
                                isLegacyChkptLocation);
                        Map<Integer, Checkpoint> ckpts = ckptMgr
                                .readCheckpoint();
                        char delim = '\t';
                        for (Map.Entry<Integer, Checkpoint> mkPair : ckpts
                                .entrySet())
                        {
                            StringBuilder output = new StringBuilder(64);
                            output.append(mkPair.getKey());
                            output.append(delim);
                            Checkpoint cp = mkPair.getValue();
                            if (cp == null)
                            {
                                output.append("null");
                            }
                            else
                            {
                                output.append(cp.toString());
                            }
                            System.out.println(output.toString());
                        }
                    }
                    else
                    {
                        throw new DatabusClusterUtilException(
                                "readSCN: please specify non-empty sources");
                    }
                }
                else if (function.equals("writeSCN"))
                {
                    String scnStr = arg1;
                    Long scn = Long.parseLong(scnStr);
                    if (partitionStr != null)
                    {
                        List<String> sources = getSources(arg2);
                        if ((sources != null) && !sources.isEmpty())
                        {
                            DatabusClusterCkptManager ckptMgr = new DatabusClusterCkptManager(
                                    zkServer, clusterName, sources, partitions,
                                    isLegacyChkptLocation);
                            ckptMgr.writeCheckpoint(scn);
                        }
                        else
                        {
                            throw new DatabusClusterUtilException(
                                    "writeSCN: please specify non-empty sources");
                        }
                    }
                    else
                    {
                        throw new DatabusClusterUtilException(
                                "writeSCN: to write the SCN to all partitions please use '-p all'");
                    }
                }
                else if (function.equals("removeSCN"))
                {
                    if (partitionStr != null)
                    {

                        List<String> sources = getSources(arg1);
                        if ((sources != null) && !sources.isEmpty())
                        {
                            DatabusClusterCkptManager ckptMgr = new DatabusClusterCkptManager(
                                    zkServer, clusterName, sources, partitions,
                                    isLegacyChkptLocation);
                            ckptMgr.remove();
                        }
                        else
                        {
                            throw new DatabusClusterUtilException(
                                    "remove: please specify non-empty sources");
                        }
                    }
                    else
                    {
                        throw new DatabusClusterUtilException(
                                "remove: to remove SCN from all partitions please use '-p all'");
                    }
                }
                else
                {
                    usage();
                    System.exit(1);
                }
            }
        }
        catch (ParseException e)
        {
            usage();
            System.exit(1);
        }
        catch (DatabusClusterUtilException e)
        {
            System.err.println("Error! " + e.toString());
            System.exit(1);
        }

    }

    private static List<String> getSources(String arg1)
    {
        if (arg1 != null)
        {
            String src = arg1.replaceAll(" ", "");
            String[] sources = src.split(",");
            List<String> sourceList = Arrays.asList(sources);
            return sourceList;
        }
        return null;
    }

    public static void usage()
    {
        System.err
                .println(" [ -z <zkAddr> -c <cluster-name>  [-p <partitionNumber1,[partionNumber2,..]>] ] [-l] FUNCTION-NAME [arglist]");
        System.err.println(" FUNCTION-NAME one of: ");
        System.err
                .println(" readSCN  [source]    :  prints the SCN written to partitions specified with -p or all if none specified for all sources");
        System.err
                .println(" writeSCN <SCN> [source]: writes the SCN written to partitions specified in -p or all if 'all' specified for all sources");
        System.err
                .println(" removeSCN [source]       : removes SCN of partitions specified in -p or all if 'all' for all sources unless specified");
        System.err
                .println(" partitions     : print the number of partitions of specified cluster");
        System.err
                .println(" clients        : print the partitions to instance mapping for partitions specified in '-p' or all if 'all' is specified ");
        System.err
                .println(" sources        : print the sources found in specified cluster ");
        System.err
                .println(" create  <numPartitions>  : create a cluster with specified number of partitions");
        System.err
                .println(" checkpoint   :  print checkpoints of partitions specified with -p or all if none specified ");
        System.err.println(" delete   : delete a cluster ");
        System.err
                .println(" Note: [source] is <src1,src2,..,srcN> corresponding to value specified in clients during subscription ");
        System.err
                .println(" Note: -l if specified writes/reads/removes checkpoints from legacy locations ");
    }

}
