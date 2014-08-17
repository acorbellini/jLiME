package edu.jlime.core.marshalling;

import org.apache.log4j.Logger;

import edu.jlime.core.cluster.Peer;
import edu.jlime.util.ByteBuffer;

public class Marshaller {

	private Logger log = Logger.getLogger(Marshaller.class);

	private TypeConverters tc;

	public Marshaller(ClassLoaderProvider cl) {
		this.tc = new TypeConverters(cl);
	}

	public Object getObject(byte[] array, Peer caller) throws Exception {
		if (log.isDebugEnabled())
			log.debug("Unmarshalling buffer of " + array.length
					+ " bytes from " + caller);

		ByteBuffer buff = new ByteBuffer(array);

		Object ret = tc.getObjectFromArray(buff);

		return ret;
	}

	public byte[] toByteArray(Object o) throws Exception {
		return toByteArray(null, o);
	}

	public byte[] toByteArray(Peer cliID, Object o) throws Exception {
		ByteBuffer buffer = new ByteBuffer();
		tc.objectToByteArray(o, buffer, cliID);
		byte[] build = buffer.build();
		if (log.isDebugEnabled()) {
			log.info("Converted " + o + " to bytes : " + build.length);
		}
		return build;
	}

	public TypeConverters getTc() {
		return tc;
	}

}
