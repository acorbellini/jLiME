package edu.jlime.graphly.server;

import java.util.ArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import edu.jlime.graphly.GraphlyConfig;
import edu.jlime.graphly.client.Graphly;
import edu.jlime.rpc.Configuration;
import edu.jlime.rpc.NetworkConfiguration;

public abstract class GraphlyServer {
	public abstract void start() throws Exception;

	public abstract void stop() throws Exception;

	public abstract Graphly getGraphlyClient() throws Exception;

	public static ArrayList<LocalGraphlyServer> createServers(final String storeDir, final Integer localServers,
			final Boolean coord, final Integer remoteServers, String netConfigFile, String graphlyConfigFile)
					throws Exception, InterruptedException {

		// LogManager.getLogManager().reset();
		// SLF4JBridgeHandler.install();
		//
		// java.util.logging.Logger.getLogger("com.sun.jersey.api.core.PackagesResourceConfig").log(Level.FINEST,
		// "");

		final ArrayList<LocalGraphlyServer> ret = new ArrayList<>();

		final NetworkConfiguration networkConfiguration = new NetworkConfiguration(
				Configuration.newConfig(netConfigFile));

		final GraphlyConfig graphlyConfiguration = new GraphlyConfig(
				Configuration.newConfig(graphlyConfigFile));

		if (localServers == 1)
			ret.add(createServer(storeDir, 0, coord, remoteServers, networkConfiguration, graphlyConfiguration));
		else {
			ExecutorService svc = Executors.newCachedThreadPool();
			for (int i = 0; i < localServers; i++) {
				final int curr = i;
				svc.execute(new Runnable() {

					@Override
					public void run() {
						try {
							boolean isCoord = false;
							if (curr == 0 && coord)
								isCoord = true;
							LocalGraphlyServer srv = createServer(storeDir, curr, isCoord, remoteServers,
									networkConfiguration, graphlyConfiguration);
							synchronized (ret) {
								ret.add(srv);
							}

						} catch (Exception e) {
							e.printStackTrace();
						}
					}
				});
			}
			svc.shutdown();
			svc.awaitTermination(Long.MAX_VALUE, TimeUnit.DAYS);
		}
		return ret;
	}

	public static LocalGraphlyServer createServer(String sdir, int i, Boolean isCoord, Integer remoteServers,
			NetworkConfiguration config, GraphlyConfig gConfig) throws Exception {
		return new LocalGraphlyServer(config, gConfig, sdir.replaceAll("\\$i", i + ""), isCoord, remoteServers);
	}

	public static void main(final String[] args) throws Exception {

		final String storeDir = args[0];
		final Integer localServers = Integer.valueOf(args[1]);
		Boolean coord = Boolean.valueOf(args[2]);
		final Integer remoteServers = Integer.valueOf(args[3]);

		ArrayList<LocalGraphlyServer> s = GraphlyServer.createServers(storeDir, localServers, coord, remoteServers,
				null, null);
		for (LocalGraphlyServer localGraphlyServer : s) {
			localGraphlyServer.start();
		}

	}

}
