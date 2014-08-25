package edu.jlime.collections.intintarray.client;

import org.apache.log4j.Logger;

import edu.jlime.collections.intintarray.db.Store;
import edu.jlime.jd.ClientNode;
import edu.jlime.jd.client.JobContext;
import edu.jlime.jd.job.Job;

public class ListJob implements Job<Boolean> {

	private String storename;

	public ListJob(String store) {
		this.storename = store;
	}

	@Override
	public Boolean call(JobContext env, ClientNode peer) throws Exception {
		Logger log = Logger.getLogger(ListJob.class);
		Store store = (Store) env.get(storename);
//		log.info(store.list());
		log.info(store.size());
		return true;
	}

}
