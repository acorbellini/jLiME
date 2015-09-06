package edu.jlime.pregel.queues;

import gnu.trove.list.array.TLongArrayList;

public interface VertexContext {
	public TLongArrayList outgoing();

	public long getVid();
}
