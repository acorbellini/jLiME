package edu.jlime.graphly.rec.salsa;

import java.io.Serializable;

import gnu.trove.map.hash.TLongFloatHashMap;

public class AuthHubSubResult implements Serializable {
	public AuthHubSubResult(TLongFloatHashMap auth, TLongFloatHashMap hub) {
		this.auth = auth;
		this.hub = hub;
	}

	public TLongFloatHashMap auth;
	public TLongFloatHashMap hub;
}
