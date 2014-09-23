package edu.jlime.core.rpc;

import java.io.Serializable;

import edu.jlime.util.ByteBuffer;

public interface RPCObject extends Serializable{
	byte[] getByteArray();

	RPCObject fromByteArray(ByteBuffer buff);
}
