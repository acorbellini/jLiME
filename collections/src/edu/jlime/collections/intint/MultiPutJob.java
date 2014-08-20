package edu.jlime.collections.intint;

import org.apache.log4j.Logger;

import edu.jlime.client.JobContext;
import edu.jlime.collections.hash.SimpleIntIntHash;
import edu.jlime.jd.ClientNode;
import edu.jlime.jd.job.RunJob;
import gnu.trove.map.hash.TIntIntHashMap;

public class MultiPutJob extends RunJob {

	private static final long serialVersionUID = 6426012614912228568L;

	private TIntIntHashMap map;

	private String hashName;

	public MultiPutJob(TIntIntHashMap subMap, String hashName) {
		this.map = subMap;
		this.hashName = hashName;
	}

	@Override
	public void run(JobContext ctx, ClientNode origin) throws Exception {
		Logger log = Logger.getLogger(MultiPutJob.class);
		log.info("Executing multiputjob, putting into hash " + map.size()
				+ " pairs.");
		SimpleIntIntHash hash = (SimpleIntIntHash) ctx.get(hashName);
		hash.adjustOrPutValue(map);
		log.info("Finished multiputjob.");
	}

}
