package edu.jlime.core.cluster;

public class DataFilter implements PeerFilter {
	private String v;
	private String k;

	public DataFilter(String k, String v) {
		this.k = k;
		this.v = v;
	}

	@Override
	public boolean verify(Peer p) {
		String data = p.getData(k);
		return (data != null && data.equals(v));
	}
}