package edu.jlime.graphly.rec;

import edu.jlime.graphly.client.GraphlyGraph;
import edu.jlime.graphly.traversal.Dir;
import edu.jlime.jd.ClientNode;
import edu.jlime.jd.client.JobContext;
import edu.jlime.jd.job.Job;
import gnu.trove.iterator.TLongIterator;
import gnu.trove.list.array.TLongArrayList;
import gnu.trove.set.hash.TLongHashSet;

public class JaccardJob implements Job<JaccardSubResult> {

	private GraphlyGraph g;
	private TLongArrayList list;

	public JaccardJob(GraphlyGraph graph, TLongArrayList value) {
		this.g = graph;
		this.list = value;
	}

	@Override
	public JaccardSubResult call(JobContext env, ClientNode peer)
			throws Exception {
		JaccardSubResult ret = new JaccardSubResult();
		TLongIterator it = list.iterator();
		while (it.hasNext()) {
			long v = it.next();
			TLongHashSet vres = new TLongHashSet(g.getEdges(Dir.BOTH, v));
			if (ret.intersect == null)
				ret.intersect = new TLongHashSet(vres);
			else {
				TLongIterator retIt = ret.intersect.iterator();
				while (retIt.hasNext()) {
					if (!vres.contains(retIt.next()))
						retIt.remove();
				}
			}
			if (ret.union == null)
				ret.union = new TLongHashSet(vres);
			else
				ret.union.addAll(vres);
		}
		return ret;
	}

}
