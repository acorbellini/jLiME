package edu.jlime.pregel.worker;

import edu.jlime.pregel.queues.VertexContext;
import gnu.trove.list.array.TLongArrayList;

public class VertexCtxImpl implements VertexContext {

	private TLongArrayList out;
	private long vid;

	public VertexCtxImpl(long vid, TLongArrayList outgoing) {
		this.vid = vid;
		this.out = outgoing;
	}

	@Override
	public TLongArrayList outgoing() {
		return this.out;
	}

	public long getVid() {
		return vid;
	}
}
