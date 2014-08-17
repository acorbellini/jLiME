package edu.jlime.core.marshalling;

import java.util.HashMap;
import java.util.UUID;

import javax.lang.model.type.NullType;

import org.apache.log4j.Logger;

import edu.jlime.core.cluster.Peer;
import edu.jlime.core.rpc.MethodCall;
import edu.jlime.core.transport.Address;
import edu.jlime.util.ByteBuffer;

public class TypeConverters {

	private HashMap<String, TypeConverter> convs = new HashMap<>();

	private HashMap<String, Byte> ids = new HashMap<>();

	private HashMap<Byte, String> types = new HashMap<>();

	private byte count = 0;

	ClassLoaderProvider clp;

	public TypeConverters(ClassLoaderProvider cl) {
		this.clp = cl;
		registerTypeConverter(Integer.class, new IntegerConverter());

		registerTypeConverter(Boolean.class, new BooleanConverter());

		registerTypeConverter(String.class, new StringConverter());

		registerTypeConverter(NullType.class, new NullConverter());

		registerTypeConverter(Address.class, new AddressConverter());

		registerTypeConverter(Object.class, new GenericObjectConverter(this));

		registerTypeConverter(MethodCall.class, new MethodCallConverter(this));

		registerTypeConverter(Peer.class, new PeerConverter(this));

		registerTypeConverter(UUID.class, new UUIDConverter());

		registerTypeConverter(byte[].class, new ByteArrayConverter());
	}

	public void registerTypeConverter(Class<?> classObj, TypeConverter conv) {
		byte id = count++;
		String className = classObj.getName();

		types.put(id, className);

		ids.put(className, id);
		convs.put(className, conv);
	}

	public TypeConverter getTypeConverter(Class<?> classObj) {
		String className = classObj.getName();
		return convs.get(className);
	}

	public Byte getTypeId(Class<?> classObj) {
		String className = classObj.getName();
		return ids.get(className);
	}

	Logger log = Logger.getLogger(TypeConverters.class);

	public void objectToByteArray(Object o, ByteBuffer buffer, Peer client)
			throws Exception {

		int size = buffer.size();
		Class<?> classOfObject = o == null ? NullType.class : o.getClass();
		// Default converter
		TypeConverter converter = getTypeConverter(classOfObject);
		Byte type = getTypeId(classOfObject);
		if (converter == null) {
			converter = getTypeConverter(Object.class);
			type = getTypeId(Object.class);
		}
		buffer.put(type);
		converter.toArray(o, buffer, client);

		if (log.isDebugEnabled())
			log.info("Converted " + o + " of type " + classOfObject + " in "
					+ (buffer.size() - size) + " bytes ");

	}

	public Object getObjectFromArray(ByteBuffer buff) throws Exception {
		byte type = buff.get();
		String className = types.get(type);
		TypeConverter converter = convs.get(className);
		return converter.fromArray(buff);
	}
}
