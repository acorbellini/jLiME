package edu.jlime.pregel.worker;

import java.io.Serializable;

public class PregelMessage implements Comparable<PregelMessage>, Serializable {

	long from;

	long to;

	Object v;

	public PregelMessage(long from, long to, Object val) {
		this.from = from;
		this.to = to;
		this.v = val;
	}

	public Object getV() {
		return v;
	}

	public long getTo() {
		return to;
	}

	public long getFrom() {
		return from;
	}

	@Override
	public String toString() {
		return "PregelMessage [from=" + from + ", to=" + to + ", v=" + v + "]";
	}

	@Override
	public int compareTo(PregelMessage o) {
		int compare = Long.compare(to, o.to);
		if (compare == 0)
			compare = Long.compare(from, o.from);

		return compare;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + (int) (from ^ (from >>> 32));
		result = prime * result + (int) (to ^ (to >>> 32));
		result = prime * result + ((v == null) ? 0 : v.hashCode());
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
		PregelMessage other = (PregelMessage) obj;
		if (from != other.from)
			return false;
		if (to != other.to)
			return false;
		if (v == null) {
			if (other.v != null)
				return false;
		} else if (!v.equals(other.v))
			return false;
		return true;
	}

	public void setV(Object v) {
		this.v = v;
	}

}