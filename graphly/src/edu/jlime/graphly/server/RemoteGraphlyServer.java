package edu.jlime.graphly.server;

import edu.jlime.graphly.client.Graphly;
import edu.jlime.util.CommandLineUtils;

public class RemoteGraphlyServer extends GraphlyServer {

	private String installPath;
	private String clusterFile;
	private String username;
	private int servers = 0;

	public RemoteGraphlyServer(String installPath, String clusterfile, String username) {
		this.installPath = installPath;
		this.clusterFile = clusterfile;
		this.username = username;
	}

	@Override
	public void start() throws Exception {
		CommandLineUtils.execCommand("bash " + installPath + "/graphly.sh start " + clusterFile + " " + username);
	}

	@Override
	public void stop() throws Exception {
		CommandLineUtils.execCommand("bash " + installPath + "/graphly.sh stop " + clusterFile + " " + username);
	}

	@Override
	public Graphly getGraphlyClient() throws Exception {
		return Graphly.build(servers);
	}

}
