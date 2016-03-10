package edu.jlime.graphly.rec.salsa;

import java.io.Serializable;

import edu.jlime.core.rpc.RPCObject;
import edu.jlime.util.ByteBuffer;

public class AuthHubSubResult implements Serializable, RPCObject {
	public long[] auth;
	public float[] auth_vals;

	public long[] hub;
	public float[] hub_vals;

	public AuthHubSubResult() {
	}

	public AuthHubSubResult(long[] auth, float[] auth_vals, long[] hub,
			float[] hub_vals) {
		super();
		this.auth = auth;
		this.auth_vals = auth_vals;
		this.hub = hub;
		this.hub_vals = hub_vals;
	}

	@Override
	public byte[] getByteArray() {
		ByteBuffer buff = new ByteBuffer(
				4 * 2 * auth.length + 4 * 2 * hub.length);
		buff.putLongArray(auth);
		buff.putFloatArray(auth_vals);
		buff.putLongArray(hub);
		buff.putFloatArray(hub_vals);
		return buff.build();
	}

	@Override
	public RPCObject fromByteArray(ByteBuffer buff) {
		auth = buff.getLongArray();
		auth_vals = buff.getFloatArray();
		hub = buff.getLongArray();
		hub_vals = buff.getFloatArray();
		return this;
	}

}
