package edu.jlime.graphly.server;

import java.util.HashMap;
import java.util.Map;

import edu.jlime.collections.intintarray.db.LevelDb;
import edu.jlime.collections.intintarray.db.Store;
import edu.jlime.core.server.JLiME;
import edu.jlime.graphly.GraphlyStoreNode;
import edu.jlime.jd.JobDispatcher;
import edu.jlime.rpc.JLiMEFactory;

public class GraphlyServer {

	private Store s;

	public GraphlyServer(Store s) {
		this.s = s;
	}

	public static void main(String[] args) throws Exception {
		new GraphlyServer(new LevelDb(args[0], args[1])).start();
	}

	private void start() throws Exception {
		Map<String, String> data = new HashMap<>();
		data.put("type", "server");

		JLiME jlime = new JLiME(new JLiMEFactory(), data);
		jlime.start();

		jlime.getRpc().registerTarget("jobdispatcher",
				new JobDispatcher(0, jlime.getRpc()), false);
		jlime.getRpc().registerTarget("graph-store", new GraphlyStoreNode(s),
				false);

	}
}
