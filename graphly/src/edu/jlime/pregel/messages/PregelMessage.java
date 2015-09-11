package edu.jlime.pregel.messages;

import java.io.Serializable;

public abstract class PregelMessage implements Comparable<PregelMessage>, Serializable {

	protected boolean broadcast = false;

	protected long from;

	protected long to;

	private String type;

	private String subgraph;

	public PregelMessage(String msgType, long from, long to) {
		this.type = msgType;
		this.from = from;
		this.to = to;
	}

	public void setSubgraph(String subgraph) {
		this.subgraph = subgraph;
	}

	public String getSubgraph() {
		return subgraph;
	}

	public void setBroadcast(boolean broadcast) {
		this.broadcast = broadcast;
	}

	public boolean isBroadcast() {
		return broadcast;
	}

	public long getTo() {
		return to;
	}

	public long getFrom() {
		return from;
	}

	public String getType() {
		return type;
	}

	@Override
	public String toString() {
		return "PregelMessage [type=" + getType() + ", from=" + getFrom() + ", to=" + to + "]";
	}

	@Override
	public int compareTo(PregelMessage o) {
		int compare = Long.compare(from, o.from);
		if (compare == 0)
			compare = Long.compare(to, o.to);

		return compare;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + (int) (getFrom() ^ (getFrom() >>> 32));
		result = prime * result + (int) (to ^ (to >>> 32));
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		GenericPregelMessage other = (GenericPregelMessage) obj;
		if (getFrom() != other.getFrom())
			return false;
		if (to != other.to)
			return false;
		return true;
	}

	public void setFrom(long from) {
		this.from = from;
	}

	public abstract PregelMessage getCopy();

	public void setTo(long to) {
		this.to = to;
	}

}