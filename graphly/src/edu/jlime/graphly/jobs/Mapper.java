package edu.jlime.graphly.jobs;

import java.io.Serializable;
import java.util.List;

import edu.jlime.graphly.util.Pair;
import edu.jlime.jd.ClientNode;
import edu.jlime.jd.client.JobContext;
import gnu.trove.list.array.TLongArrayList;

public interface Mapper extends Serializable {

	public List<Pair<ClientNode, TLongArrayList>> map(int max, long[] data,
			JobContext ctx) throws Exception;

	public String getName();

}