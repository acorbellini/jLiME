package edu.jlime.core.marshalling;

import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

import javax.lang.model.type.NullType;

import org.apache.log4j.Logger;

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
import edu.jlime.core.rpc.RPC;
import edu.jlime.core.rpc.RPCObject;
import edu.jlime.core.transport.Address;
import edu.jlime.metrics.metric.Metrics;
import edu.jlime.util.ByteBuffer;
import gnu.trove.map.hash.TLongFloatHashMap;

public class TypeConverters {

	// private HashMap<String, TypeConverter> convs = new HashMap<>();

	private Map<String, TypeConverter> convs = new ConcurrentHashMap<>();

	RPC rpc;

	public RPC getRPC() {
		return rpc;
	}

	public TypeConverters(RPC rpcDispatcher) {

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

		registerTypeConverter(RPC.class, new RPCDispatcherConverter(rpc));

		registerTypeConverter(RPCObject.class, new RPCObjectConverter(this));

		registerTypeConverter(Peer.class, new PeerConverter(this));

		registerTypeConverter(UUID.class, new UUIDConverter());

		registerTypeConverter(Metrics.class, new MetricConverter(this));

		registerTypeConverter(byte[].class, new ByteArrayConverter());

		registerTypeConverter(int[].class, new IntArrayConverter());

		registerTypeConverter(long[].class, new LongArrayConverter());

		registerTypeConverter(float[].class, new FloatArrayConverter());

		registerTypeConverter(double[].class, new DoubleArrayConverter());

		registerTypeConverter(TLongFloatHashMap.class,
				new TLongFloatMapConverter());

	}

	/**
	 * 
	 * @param class
	 *            of the object to be marshalled/unmarshalled
	 * @param converter
	 *            to be used
	 */
	public void registerTypeConverter(Class<?> classObj, TypeConverter conv) {
		String className = classObj.getName();
		convs.put(className, conv);

	}

	public TypeConverter getTypeConverter(Class<?> classObj) {
		String className = classObj.getName();
		return convs.get(className);
	}

	Logger log = Logger.getLogger(TypeConverters.class);

	public void objectToByteArray(Object o, ByteBuffer buffer, Peer client)
			throws Exception {
		Class<?> classOfObject = o == null ? NullType.class : o.getClass();
		// Default converter
		TypeConverter converter = getTypeConverter(classOfObject);
		String name = classOfObject.getName();
		if (converter == null) {
			if (RPCObject.class.isAssignableFrom(classOfObject)) {
				converter = getTypeConverter(RPCObject.class);
				name = RPCObject.class.getName();
			} else {
				converter = getTypeConverter(Object.class);
				name = Object.class.getName();
			}
		}
		buffer.putString(name);
		converter.toArray(o, buffer, client);

		// if (log.isDebugEnabled())
		// log.info("Converted " + o + " of type " + classOfObject + " in "
		// + (buffer.size() - size) + " bytes ");

	}

	public Object getObjectFromArray(ByteBuffer buff) throws Exception {
		String type = "";
		try {
			type = buff.getString();
			if (!convs.containsKey(type)) {
				log.error("Converter for type " + type + " not found.");
			}
			TypeConverter converter = convs.get(type);
			return converter.fromArray(buff);
		} catch (Exception e) {
			throw new Exception(
					"Exception getting object from array of bytes for type "
							+ type,
					e);
		}
	}

	public void clear() {
		convs.clear();
	}
}
