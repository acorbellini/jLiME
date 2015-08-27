package edu.jlime.graphly.rec.salsa;

import gnu.trove.map.hash.TLongFloatHashMap;

import java.io.Serializable;

public class AuthHubSubResult implements Serializable {
	public AuthHubSubResult(TLongFloatHashMap auth2, TLongFloatHashMap hub2) {
		this.auth = auth2;
		this.hub = hub2;
	}

	public TLongFloatHashMap auth;
	public TLongFloatHashMap hub;
}
