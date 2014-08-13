package edu.jlime.core.marshalling;

import edu.jlime.util.ByteBuffer;

public interface TypeConverter {

	void toArray(Object o, ByteBuffer buffer) throws Exception;

	Object fromArray(ByteBuffer buff, String originID, String clientID)
			throws Exception;
}