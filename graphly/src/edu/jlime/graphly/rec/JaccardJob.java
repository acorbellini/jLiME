package edu.jlime.graphly.rec;

import edu.jlime.graphly.client.Graph;
import edu.jlime.graphly.traversal.Dir;
import edu.jlime.jd.Node;
import edu.jlime.jd.client.JobContext;
import edu.jlime.jd.job.Job;
import gnu.trove.iterator.TLongIterator;
import gnu.trove.list.array.TLongArrayList;
import gnu.trove.set.hash.TLongHashSet;

public class JaccardJob implements Job<JaccardSubResult> {

	private Graph g;
	private TLongArrayList list;

	public JaccardJob(Graph graph, TLongArrayList value) {
		this.g = graph;
		this.list = value;
	}

	@Override
	public JaccardSubResult call(JobContext env, Node peer) throws Exception {
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
