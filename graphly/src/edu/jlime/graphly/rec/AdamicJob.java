package edu.jlime.graphly.rec;

import edu.jlime.graphly.client.Graph;
import edu.jlime.graphly.traversal.Dir;
import edu.jlime.jd.Node;
import edu.jlime.jd.client.JobContext;
import edu.jlime.jd.job.Job;
import gnu.trove.iterator.TLongIterator;
import gnu.trove.list.array.TLongArrayList;

public class AdamicJob implements Job<Float> {

	private TLongArrayList list;
	private Graph g;

	public AdamicJob(Graph graph, TLongArrayList value) {
		this.list = value;
		this.g = graph;
	}

	@Override
	public Float call(JobContext env, Node peer) throws Exception {
		float count = 0f;
		TLongIterator it = list.iterator();
		while (it.hasNext()) {
			long v = it.next();
			count += (float) (1 / Math.log(g.getEdgesCount(Dir.BOTH, v, null)));
		}
		return count;
	}

}
