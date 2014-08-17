package edu.jlime.core.marshalling;

import java.io.ByteArrayInputStream;

import edu.jlime.core.cluster.Peer;
import edu.jlime.util.ByteBuffer;

final class GenericObjectConverter implements TypeConverter {
	/**
	 * 
	 */
	private final TypeConverters typeConverters;

	/**
	 * @param typeConverters
	 */
	GenericObjectConverter(TypeConverters typeConverters) {
		this.typeConverters = typeConverters;
	}

	@Override
	public void toArray(Object o, ByteBuffer buff, Peer cliID)
			throws Exception {
		this.typeConverters.objectToByteArray(cliID, buff, null);
		buff.putObject(o);
	}

	@Override
	public Object fromArray(ByteBuffer buff) throws Exception {
		Peer client = (Peer) this.typeConverters.getObjectFromArray(buff);
		ByteArrayInputStream bis = new ByteArrayInputStream(
				buff.getByteArray());

		MarshallerInputStream stream = new MarshallerInputStream(bis, this.typeConverters.clp,
				client);
		Object ret = stream.readObject();
		stream.close();
		return ret;
	}
}