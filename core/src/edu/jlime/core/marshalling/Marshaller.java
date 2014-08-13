package edu.jlime.core.marshalling;

import org.apache.log4j.Logger;

import edu.jlime.util.ByteBuffer;
import edu.jlime.util.DEFByteArrayCache;

public class Marshaller {

	private Logger log = Logger.getLogger(Marshaller.class);

	private TypeConverters tc;

	public Marshaller(ClassLoaderProvider cl) {
		this.tc = new TypeConverters(cl);
	}

	public Object getObject(byte[] array, String originID) throws Exception {
		if (log.isDebugEnabled())
			log.debug("Unmarshalling buffer of "
					+ (array.length / (float) 1024) + " kb from " + originID);
		// DEFByteBuffer buff = new
		// DEFByteBuffer(Compression.uncompress(array));

		ByteBuffer buff = new ByteBuffer(array);

		String clientID = buff.getString();

		Object ret = tc.getObjectFromArray(buff, originID, clientID);

		return ret;
	}

	public byte[] toByteArray(Object o) throws Exception {
		return toByteArray(o, "");
	}

	public byte[] toByteArray(Object o, String cliID) throws Exception {
		ByteBuffer buffer = new ByteBuffer();
		buffer.putString(cliID);
		tc.objectToByteArray(o, buffer);
		// return Compression.compress(buffer.build());
		return buffer.build();
	}

	public TypeConverters getTc() {
		return tc;
	}

}
