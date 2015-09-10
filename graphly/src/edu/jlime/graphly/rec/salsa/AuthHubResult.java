package edu.jlime.graphly.rec.salsa;

import edu.jlime.graphly.traversal.CountResult;
import edu.jlime.graphly.traversal.TraversalResult;
import gnu.trove.map.TLongFloatMap;
import gnu.trove.map.hash.TLongFloatHashMap;
import gnu.trove.set.hash.TLongHashSet;

public class AuthHubResult extends TraversalResult {

	private CountResult hub;
	private CountResult auth;

	public AuthHubResult(TLongFloatHashMap auth, TLongFloatHashMap hub) {
		this.hub = new CountResult(hub);
		this.auth = new CountResult(auth);
	}

	@Override
	public TLongHashSet vertices() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public TraversalResult removeAll(TLongHashSet v) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public TraversalResult retainAll(TLongHashSet v) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public float getCount(long key) throws Exception {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public TLongFloatMap getCounts() throws Exception {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public TraversalResult top(int top) throws Exception {
		hub = (CountResult) hub.top(top);
		auth = (CountResult) auth.top(top);
		return this;
	}

	@Override
	public String toString() {
		StringBuilder builder = new StringBuilder();
		builder.append("Auth: \n" + auth.toString() + "\n");
		builder.append("Hub: \n" + hub.toString() + "\n");
		return builder.toString();
	}

}
