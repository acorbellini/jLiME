package edu.jlime.graphly.jobs;

import java.io.Serializable;
import java.util.Map;

import edu.jlime.jd.ClientNode;
import edu.jlime.jd.client.JobContext;
import gnu.trove.list.array.TLongArrayList;

public abstract class Mapper implements Serializable {

	private static final long serialVersionUID = 6201330433508707230L;

	public abstract Map<ClientNode, TLongArrayList> map(long[] data,
			JobContext ctx) throws Exception;

}