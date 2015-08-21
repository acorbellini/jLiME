package edu.jlime.graphly.rec.salsa;

import java.io.Serializable;

import gnu.trove.map.hash.TLongFloatHashMap;

public class AuthHubSubResult implements Serializable {
	public AuthHubSubResult(TLongFloatHashMap auth2, TLongFloatHashMap hub2) {
		this.auth = auth2;
		this.hub = hub2;
	}

	public TLongFloatHashMap auth;
	public TLongFloatHashMap hub;
}
