package edu.jlime.collections.adjacencygraph;

import org.apache.log4j.Logger;

import edu.jlime.collections.intintarray.client.PersistentIntIntArrayMap;
import edu.jlime.collections.intintarray.client.jobs.StoreConfig;
import edu.jlime.jd.ClientNode;
import edu.jlime.jd.client.JobContext;
import edu.jlime.jd.job.Job;

public class AdyacencyGraphInitJob implements Job<Boolean> {

	private static final long serialVersionUID = 5818355701790659969L;

	private String storeID;

	private StoreConfig storeConfig;

	public AdyacencyGraphInitJob(String storeID, StoreConfig storeConfig) {
		this.storeID = storeID;
		this.storeConfig = storeConfig;
	}

	@Override
	public Boolean call(JobContext ctx, ClientNode peer) throws Exception {
		Logger log = Logger.getLogger(RemoteAdjacencyGraph.class);
		// if (log.isDebugEnabled())
		log.info("Initializing adyacency graph, requested from peer " + peer);
		ctx.put(storeID,
				new PersistentIntIntArrayMap(storeConfig, ctx.getCluster()));
		if (log.isDebugEnabled())
			log.debug("Finished initialization of adyacency graph, requested from peer "
					+ peer);
		return true;
	}
}