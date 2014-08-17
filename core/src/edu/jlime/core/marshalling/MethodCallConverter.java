package edu.jlime.core.marshalling;

import java.util.ArrayList;
import java.util.List;

import edu.jlime.core.cluster.Peer;
import edu.jlime.core.rpc.MethodCall;
import edu.jlime.util.ByteBuffer;

final class MethodCallConverter implements TypeConverter {
	/**
	 * 
	 */
	private final TypeConverters typeConverters;

	/**
	 * @param typeConverters
	 */
	MethodCallConverter(TypeConverters typeConverters) {
		this.typeConverters = typeConverters;
	}

	@Override
	public void toArray(Object o, ByteBuffer buff, Peer cliID)
			throws Exception {
		MethodCall mc = (MethodCall) o;
		buff.putString(mc.getName());
		buff.putString(mc.getObjectKey());

		Object[] objects = mc.getObjects();
		buff.putInt(objects.length);
		for (Object object : objects)
			this.typeConverters.objectToByteArray(object, buff, cliID);
	}

	@Override
	public Object fromArray(ByteBuffer buff) throws Exception {
		List<Object> objects = new ArrayList<>();
		String name = buff.getString();
		String k = buff.getString();
		int num = buff.getInt();
		for (int j = 0; j < num; j++)
			objects.add(this.typeConverters.getObjectFromArray(buff));
		return new MethodCall(k, name, objects.toArray());
	}
}