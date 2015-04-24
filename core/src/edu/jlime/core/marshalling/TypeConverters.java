package edu.jlime.core.marshalling;

import edu.jlime.core.cluster.Peer;
import edu.jlime.core.marshalling.converters.AddressConverter;
import edu.jlime.core.marshalling.converters.BooleanConverter;
import edu.jlime.core.marshalling.converters.ByteArrayConverter;
import edu.jlime.core.marshalling.converters.GenericObjectConverter;
import edu.jlime.core.marshalling.converters.IntArrayConverter;
import edu.jlime.core.marshalling.converters.IntegerConverter;
import edu.jlime.core.marshalling.converters.MethodCallConverter;
import edu.jlime.core.marshalling.converters.MetricConverter;
import edu.jlime.core.marshalling.converters.NullConverter;
import edu.jlime.core.marshalling.converters.PeerConverter;
import edu.jlime.core.marshalling.converters.StringConverter;
import edu.jlime.core.marshalling.converters.UUIDConverter;
import edu.jlime.core.rpc.MethodCall;
import edu.jlime.core.rpc.RPCDispatcher;
import edu.jlime.core.rpc.RPCObject;
import edu.jlime.core.transport.Address;
import edu.jlime.metrics.metric.Metrics;
import edu.jlime.util.ByteBuffer;
import gnu.trove.map.hash.TObjectByteHashMap;

import java.util.ArrayList;
import java.util.UUID;

import javax.lang.model.type.NullType;

import org.apache.log4j.Logger;

public class TypeConverters {

	// private HashMap<String, TypeConverter> convs = new HashMap<>();

	private ArrayList<TypeConverter> convs = new ArrayList<>();

	private TObjectByteHashMap<String> ids = new TObjectByteHashMap<String>();

	private ArrayList<String> types = new ArrayList<>();

	private byte count = 0;

	RPCDispatcher rpc;

	public RPCDispatcher getRPC() {
		return rpc;
	}

	public TypeConverters(RPCDispatcher rpcDispatcher) {

		this.rpc = rpcDispatcher;

		registerTypeConverter(Long.class, new LongConverter());

		registerTypeConverter(Double.class, new DoubleConverter());

		registerTypeConverter(Integer.class, new IntegerConverter());

		registerTypeConverter(Boolean.class, new BooleanConverter());

		registerTypeConverter(String.class, new StringConverter());

		registerTypeConverter(NullType.class, new NullConverter());

		registerTypeConverter(Address.class, new AddressConverter());

		registerTypeConverter(Object.class, new GenericObjectConverter(this));

		registerTypeConverter(MethodCall.class, new MethodCallConverter(this));

		registerTypeConverter(RPCDispatcher.class, new RPCDispatcherConverter(
				rpc));

		registerTypeConverter(RPCObject.class, new RPCObjectConverter(this));

		registerTypeConverter(Peer.class, new PeerConverter(this));

		registerTypeConverter(UUID.class, new UUIDConverter());

		registerTypeConverter(Metrics.class, new MetricConverter(this));

		registerTypeConverter(byte[].class, new ByteArrayConverter());

		registerTypeConverter(int[].class, new IntArrayConverter());

		registerTypeConverter(long[].class, new LongArrayConverter());

		registerTypeConverter(float[].class, new FloatArrayConverter());

		registerTypeConverter(double[].class, new DoubleArrayConverter());

	}

	public void registerTypeConverter(Class<?> classObj, TypeConverter conv) {
		byte id = count++;
		String className = classObj.getName();

		types.add(className);
		convs.add(conv);

		ids.put(className, id);

	}

	public TypeConverter getTypeConverter(Class<?> classObj) {
		String className = classObj.getName();
		byte index = ids.get(className);
		if (index == ids.getNoEntryValue())
			return null;
		return convs.get(index);
	}

	public byte getTypeId(Class<?> classObj) {
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
		byte type = getTypeId(classOfObject);
		if (converter == null) {

			if (RPCObject.class.isAssignableFrom(classOfObject)) {
				converter = getTypeConverter(RPCObject.class);
				type = getTypeId(RPCObject.class);
			} else {
				converter = getTypeConverter(Object.class);
				type = getTypeId(Object.class);
			}
		}
		buffer.put(type);
		converter.toArray(o, buffer, client);

		// if (log.isDebugEnabled())
		// log.info("Converted " + o + " of type " + classOfObject + " in "
		// + (buffer.size() - size) + " bytes ");

	}

	public Object getObjectFromArray(ByteBuffer buff) throws Exception {
		byte type = buff.get();
		// String className = types.get(type);
		if (convs.size() <= type) {
			log.error("Converter for type " + type + " not found.");
		}
		TypeConverter converter = convs.get(type);
		return converter.fromArray(buff);
	}
}
