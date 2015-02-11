package edu.jlime.graphly.traversal;

import edu.jlime.collections.adjacencygraph.get.Dir;
import edu.jlime.graphly.jobs.Mapper;
import edu.jlime.jd.JobDispatcher;
import gnu.trove.map.hash.TLongIntHashMap;

public class CountStep implements Step<long[], TLongIntHashMap> {

	private Dir dir;
	private GraphlyTraversal gt;

	public CountStep(Dir dir, GraphlyTraversal gt) {
		this.dir = dir;
		this.gt = gt;
	}

	@Override
	public TLongIntHashMap exec(long[] before) throws Exception {
		JobDispatcher te = gt.getGraph().getJobClient();
		CountForkJoin vfj = new CountForkJoin(before, dir,
				(Mapper) gt.get("mapper"));
		TLongIntHashMap ret = vfj.exec(te.getCluster());
		return ret;
	}

}
