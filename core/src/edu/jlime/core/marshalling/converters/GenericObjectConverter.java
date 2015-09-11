package edu.jlime.core.marshalling.converters;

import java.io.ByteArrayInputStream;

import edu.jlime.core.cluster.Peer;
import edu.jlime.core.marshalling.MarshallerInputStream;
import edu.jlime.core.marshalling.TypeConverter;
import edu.jlime.core.marshalling.TypeConverters;
import edu.jlime.util.ByteBuffer;

public class GenericObjectConverter implements TypeConverter {
	/**
	 * 
	 */
	private final TypeConverters typeConverters;

	/**
	 * @param typeConverters
	 */
	public GenericObjectConverter(TypeConverters typeConverters) {
		this.typeConverters = typeConverters;
	}

	@Override
	public void toArray(Object o, ByteBuffer buff, Peer cliID) throws Exception {
		this.typeConverters.objectToByteArray(cliID, buff, null);
		buff.putObject(o);
	}

	@Override
	public Object fromArray(ByteBuffer buff) throws Exception {
		Peer client = (Peer) this.typeConverters.getObjectFromArray(buff);
		ByteArrayInputStream bis = new ByteArrayInputStream(buff.getByteArray());

		MarshallerInputStream stream = new MarshallerInputStream(bis, this.typeConverters.getRPC(), client);
		Object ret = stream.readObject();
		stream.close();
		return ret;
	}
}