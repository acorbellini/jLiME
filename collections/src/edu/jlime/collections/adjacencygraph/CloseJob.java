package edu.jlime.collections.adjacencygraph;

import org.apache.log4j.Logger;

import edu.jlime.collections.intintarray.client.PersistentIntIntArrayMap;
import edu.jlime.collections.intintarray.client.jobs.StoreConfig;
import edu.jlime.jd.ClientNode;
import edu.jlime.jd.client.JobContext;
import edu.jlime.jd.job.RunJob;

public class CloseJob extends RunJob {

	private static final long serialVersionUID = -6149239532205385838L;

	private String map;

	public CloseJob(String map, StoreConfig config) {
		this.map = map;
	}

	@Override
	public void run(JobContext ctx, ClientNode origin) throws Exception {
		Logger log = Logger.getLogger(CloseJob.class);
		log.info("Executing graph closing job.");
		PersistentIntIntArrayMap pmap = (PersistentIntIntArrayMap) ctx
				.remove(map);
		if (pmap != null)
			pmap.close();
	}
}
