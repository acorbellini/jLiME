package edu.jlime.graphly.jobs;

import java.io.Serializable;
import java.util.List;

import edu.jlime.core.cluster.Peer;
import edu.jlime.jd.Node;
import edu.jlime.jd.client.JobContext;
import edu.jlime.util.Pair;
import gnu.trove.list.array.TLongArrayList;

public interface Mapper extends Serializable {

	public List<Pair<Node, TLongArrayList>> map(int max, long[] data, JobContext ctx) throws Exception;

	public String getName();

	public boolean isDynamic();

	public void update(JobContext ctx) throws Exception;

	public Node getNode(long v, JobContext ctx);

	public Peer[] getPeers();

	public int hash(long to, JobContext ctx);

}