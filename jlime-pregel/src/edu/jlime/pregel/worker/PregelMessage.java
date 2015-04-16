package edu.jlime.pregel.worker;

import java.io.Serializable;

public abstract class PregelMessage implements Comparable<PregelMessage>,
		Serializable {

	protected boolean broadcast = false;

	protected long from;

	public abstract void setV(Object v);

	public abstract Object getV();

	protected long to;

	public PregelMessage(long from, long to) {
		this.from = from;
		this.to = to;
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

	@Override
	public String toString() {
		return "PregelMessage [from=" + getFrom() + ", to=" + to + ", v="
				+ getV() + "]";
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