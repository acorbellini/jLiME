package edu.jlime.jd;

import edu.jlime.core.cluster.Peer;
import edu.jlime.core.marshalling.TypeConverter;
import edu.jlime.core.marshalling.TypeConverters;
import edu.jlime.util.ByteBuffer;

public class RemoteReferenceConverter implements TypeConverter {
	private final TypeConverters tc;

	RemoteReferenceConverter(TypeConverters tc) {
		this.tc = tc;
	}

	@Override
	public void toArray(Object o, ByteBuffer buffer, Peer cliID)
			throws Exception {
		RemoteReference rr = (RemoteReference) o;
		tc.objectToByteArray(rr.getNode(), buffer, cliID);
		buffer.putString(rr.getKey());
	}

	@Override
	public Object fromArray(ByteBuffer buff) throws Exception {
		ClientNode p = (ClientNode) tc.getObjectFromArray(buff);
		String key = buff.getString();
		RemoteReference rr = new RemoteReference(p, key);
		return rr;
	}
}