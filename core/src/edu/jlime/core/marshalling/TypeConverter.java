package edu.jlime.core.marshalling;

import edu.jlime.core.cluster.Peer;
import edu.jlime.util.ByteBuffer;

public interface TypeConverter {

	void toArray(Object o, ByteBuffer buffer, Peer cliID) throws Exception;

	Object fromArray(ByteBuffer buff, Peer originID) throws Exception;
}