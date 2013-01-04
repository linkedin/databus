package com.linkedin.databus2.client.util;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;

import com.linkedin.databus.client.DatabusClientDSCUpdater;
import com.linkedin.databus.client.pub.DatabusClientGroupMember;
import com.linkedin.databus.client.pub.DatabusClientNode;
import com.linkedin.databus.core.Checkpoint;
import com.linkedin.databus.core.DbusClientMode;
import com.linkedin.databus.core.util.InvalidConfigException;


public class DbusClientClusterUtil {

	
	
	
	/**
	 * @param args
	 * DbusClientClusterUtil -s <serverList> -n <namespace> -g <group> -d <dir>     members
	 * 													   	 		leader 
	 * 													  	 		keys 
	 * 													  	 		readSCN <key> 
	 * 													  	 		writeSCN <key> SCN [OFFSET]
	 * 																remove <key>
	 * 																readLastTS
	 * 																writeLastTS TIMESTAMP				
	 */
	public static void main(String[] args) {
		try 
		{
			GnuParser cmdLineParser  = new GnuParser();
			Options options = new Options();
			options.addOption("n",true,"Zookeeper namespace  [/DatabusClient")
			.addOption("g",true,"Groupname [default-group-name] ")
			.addOption("d",true,"Shared directory name [shareddata] ")
			.addOption("s",true,"Zookeeper server list [localhost:2181] ")
			.addOption("h",false,"help");
			CommandLine cmdLineArgs  = cmdLineParser.parse(options, args,false);
			
			if (cmdLineArgs.hasOption('h')) {
				usage();
				System.exit(0);
			}
			
			String namespace = cmdLineArgs.getOptionValue('n');
			if (namespace==null || namespace.isEmpty()) 
			{
				namespace = "/DatabusClient";
			}
			String groupname = cmdLineArgs.getOptionValue('g');
			if (groupname==null || groupname.isEmpty())
			{
				groupname = "default-group-name";
			}
			String sharedDir = cmdLineArgs.getOptionValue('d');
			if (sharedDir==null || sharedDir.isEmpty())
			{
				sharedDir = "shareddata";
			}
			String serverList = cmdLineArgs.getOptionValue('s');
			if (serverList==null || serverList.isEmpty()) 
			{
				serverList  = "localhost:2181";
			}
			String[] fns =  cmdLineArgs.getArgs();
			if (fns.length < 1) 
			{
				usage();
				System.exit(1);
			}
			String function = fns[0];
			String arg1 = (fns.length > 1) ? fns[1] : null;
			String arg2 = (fns.length > 2) ? fns[2] : null;
			try 
			{
				String memberName = "cmd-line-tool";
				DatabusClientNode clusterNode = new DatabusClientNode (new DatabusClientNode.StaticConfig(true,serverList,
						2000,
						5000,
						namespace,
						groupname,
						memberName,false,sharedDir));
				DatabusClientGroupMember member = clusterNode.getMember(namespace,groupname,memberName);
				if (member == null  || !member.joinWithoutLeadershipDuties())  
				{
					System.err.println("Initialization failed for: " +  member);
					System.exit(1);
				}
				
				if (function.equals("members")) {
					List<String> mlist = member.getMembers();
					for (String m: mlist) {
						System.out.println(m);
					}

				} else if (function.equals("leader")) {
					String leader = member.getLeader();
					System.out.println(leader);

				} else if (function.equals("keys")) {
					List<String> keyList = member.getSharedKeys();
					if (keyList != null) {
						for (String m: keyList) {
							System.out.println(m);
						}
					}
				} else if (function.equals("readSCN")) {
					List<String> keyList ;
					if (arg1 == null) 
					{
						keyList = member.getSharedKeys();
					} else {
						keyList = new ArrayList<String>();
						keyList.add(arg1);
					}
					if (keyList != null) 
					{
					  for (String k: keyList)
					  {
					    if (!k.equals(DatabusClientDSCUpdater.DSCKEY))
					    {
					      Checkpoint cp = (Checkpoint) member.readSharedData(k);
					      if (cp != null)
					      {
					        System.out.println(k + " " + cp.getWindowScn() + " "+cp.getWindowOffset());
					      }
					      else
					      {
					        System.err.println(k + " null null");
					      }
					    }
					  }
					}
						
				} else if (function.equals("writeSCN")) {
					if (arg1 != null && arg2 != null) 
					{
						Checkpoint cp = new Checkpoint();
						cp.setConsumptionMode(DbusClientMode.ONLINE_CONSUMPTION);
						cp.setWindowScn(Long.parseLong(arg2));
						if (fns.length > 3) 
						{
							cp.setWindowOffset(Integer.parseInt(fns[3]));
						} else {
							cp.setWindowOffset(-1);
						}
						if (member.writeSharedData(arg1, cp))
						{
							System.out.println(arg1 + " " + cp.getWindowScn() + " " + cp.getWindowOffset());
						} 
						else 
						{
							System.err.println("Write failed! " + member +  " couldn't write key=" + arg1);
							System.exit(1);
						}
					} 
					else 
					{	
						usage();
						System.exit(1);
					}
				} else if (function.equals("readLastTs")) {
				  Long timeInMs = (Long) member.readSharedData(DatabusClientDSCUpdater.DSCKEY);
				  if (timeInMs != null) 
                  {
                    System.out.println(DatabusClientDSCUpdater.DSCKEY + " " + timeInMs.longValue());
                  } 
                  else 
                  {
                    System.err.println(DatabusClientDSCUpdater.DSCKEY + " null");
                  }
				} else if (function.equals("writeLastTs")) {
				  if (arg1 != null) 
				  {
				    Long ts = Long.parseLong(arg1);
				    if (member.writeSharedData(DatabusClientDSCUpdater.DSCKEY, ts))
                    {
                        System.out.println(DatabusClientDSCUpdater.DSCKEY + " " + ts);
                    } 
                    else 
                    {
                        System.err.println("Write failed! " + member +  " couldn't write key=" + DatabusClientDSCUpdater.DSCKEY);
                        System.exit(1);
                    }
				    
				  }
				  else
				  {
				    usage();
				    System.exit(1);
				  }
				} else if (function.equals("remove")) {
					if (!member.removeSharedData(arg1))    
					{
						System.err.println("Remove failed! " + arg1);
						System.exit(1);
					}
				} else if (function.equals("create")) {
					if (!member.createPaths())
					{
						System.err.println("Create path failed!" );
						System.exit(1);
					}
					
				} else {
					usage();
					System.exit(1);
				}
			}
			catch (InvalidConfigException e) {
				e.printStackTrace();
				usage();
				System.exit(1);
			}
		
		}
		catch (ParseException e)
		{
			usage();
			System.exit(1);
		}
		
	}
	
	
	public static void usage() 
	{
		System.err.println (" [ -n <namespace>] [-g <groupname>] [-d <shareddir>] [-s <cluster-server-list>] FUNCTION-NAME");
		System.err.println(" FUNCTION-NAME one of: ");
		System.err.println(" members : lists members belonging to the group <groupname> ");
		System.err.println(" leader: lists the leader of the group <groupname> ");
		System.err.println(" keys: lists the keys written to shared directory <shareddir> ");
		System.err.println(" readSCN [<key>] : reads the SCN written to shared directory <shareddir> ");
		System.err.println(" writeSCN <key> <SCN> [offset] : writes the SCN written to shared directory <shareddir> ");
		System.err.println(" remove <[key]> : removes key  ; all keys if none specified in <shareddir>");
		System.err.println(" readLastTs : reads the last TS");
		System.err.println(" create : create the path if none exists ");
		System.err.println(" writeLastTs <TIMESTAMP> : writes latest TS");
	}
	
	
}
