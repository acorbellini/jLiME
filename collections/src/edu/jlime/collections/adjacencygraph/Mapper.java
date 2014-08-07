package edu.jlime.collections.adjacencygraph;

import java.io.Serializable;
import java.util.Map;

import edu.jlime.client.JobContext;
import edu.jlime.jd.JobNode;
import gnu.trove.list.array.TIntArrayList;

public abstract class Mapper implements Serializable {

	private static final long serialVersionUID = 6201330433508707230L;

	public abstract Map<JobNode, TIntArrayList> map(int[] data, JobContext ctx)
			throws Exception;

}