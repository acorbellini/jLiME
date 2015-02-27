package edu.jlime.core.cluster;

public class DataFilter implements PeerFilter {
	private String v;
	private String k;
	private boolean contains;

	public DataFilter(String k, String v, boolean contains) {
		this.k = k;
		this.v = v;
		this.contains = contains;
	}

	@Override
	public boolean verify(Peer p) {
		String data = p.getData(k);
		return (data != null && contains ? data.contains(v) : data.equals(v));
	}
}