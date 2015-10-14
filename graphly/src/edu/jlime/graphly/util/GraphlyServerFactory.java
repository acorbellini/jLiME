package edu.jlime.graphly.util;

import edu.jlime.graphly.GraphlyConfig;
import edu.jlime.graphly.server.GraphlyServer;
import edu.jlime.graphly.server.LocalGraphlyServer;
import edu.jlime.graphly.server.RemoteGraphlyServer;
import edu.jlime.rpc.NetworkConfiguration;

public abstract class GraphlyServerFactory {
	public abstract GraphlyServer build() throws Exception;

	public static GraphlyServerFactory loopback(final String path) {
		return new GraphlyServerFactory() {

			@Override
			public GraphlyServer build() throws Exception {
				NetworkConfiguration net = new NetworkConfiguration();
				GraphlyConfig gconfig = new GraphlyConfig();

				net.protocol = "local";
				GraphlyServer server = LocalGraphlyServer.createServer(path, 0, true, 1, net, gconfig);
				return server;
			}
		};

	}

	public static GraphlyServerFactory distributed(final String installpath, final String username,
			final String clusterFile, final int servers) {
		return new GraphlyServerFactory() {

			@Override
			public GraphlyServer build() throws Exception {
				return new RemoteGraphlyServer(installpath, clusterFile, username, servers);
			}
		};
	}
}
