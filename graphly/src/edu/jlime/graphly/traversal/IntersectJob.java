package edu.jlime.graphly.traversal;

import edu.jlime.graphly.client.GraphlyGraph;
import edu.jlime.jd.ClientNode;
import edu.jlime.jd.client.JobContext;
import edu.jlime.jd.job.Job;
import gnu.trove.iterator.TLongIterator;
import gnu.trove.list.array.TLongArrayList;
import gnu.trove.set.hash.TLongHashSet;

public class IntersectJob implements Job<TLongHashSet> {

	private GraphlyGraph g;
	private Dir dir;
	private TLongArrayList list;

	public IntersectJob(GraphlyGraph graph, Dir dir,
			TLongArrayList tLongArrayList) {
		this.g = graph;
		this.dir = dir;
		this.list = tLongArrayList;
	}

	@Override
	public TLongHashSet call(JobContext env, ClientNode peer) throws Exception {
		TLongHashSet ret = null;
		TLongIterator it = list.iterator();
		while (it.hasNext()) {
			long v = it.next();
			TLongHashSet vres = new TLongHashSet(g.getEdges(dir, v));
			if (ret == null)
				ret = vres;
			else {
				TLongIterator retIt = ret.iterator();
				while (retIt.hasNext()) {
					if (!vres.contains(retIt.next()))
						retIt.remove();
				}
			}
		}
		return ret;
	}
}
