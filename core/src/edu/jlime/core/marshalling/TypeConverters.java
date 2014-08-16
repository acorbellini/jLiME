package edu.jlime.core.marshalling;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.UUID;

import javax.lang.model.type.NullType;

import edu.jlime.core.cluster.Peer;
import edu.jlime.core.rpc.MethodCall;
import edu.jlime.util.ByteBuffer;

public class TypeConverters {

	private HashMap<String, TypeConverter> convs = new HashMap<>();

	private HashMap<String, Byte> ids = new HashMap<>();

	private HashMap<Byte, String> types = new HashMap<>();

	private byte count = 0;

	private ClassLoaderProvider clp;

	{
		registerTypeConverter(Integer.class, new TypeConverter() {

			@Override
			public void toArray(Object o, ByteBuffer ByteBuffer, Peer cliID) {
				ByteBuffer.putInt((Integer) o);
			}

			@Override
			public Object fromArray(ByteBuffer buff, Peer originID) {
				return buff.getInt();
			}
		});

		registerTypeConverter(Boolean.class, new TypeConverter() {

			@Override
			public void toArray(Object o, ByteBuffer buff, Peer cliID) {
				buff.putBoolean((Boolean) o);
			}

			@Override
			public Object fromArray(ByteBuffer buff, Peer originID) {
				return buff.getBoolean();
			}
		});

		registerTypeConverter(String.class, new TypeConverter() {

			@Override
			public void toArray(Object o, ByteBuffer buff, Peer cliID) {
				buff.putString((String) o);
			}

			@Override
			public Object fromArray(ByteBuffer buff, Peer originID) {
				return buff.getString();
			}
		});

		registerTypeConverter(NullType.class, new TypeConverter() {

			@Override
			public void toArray(Object o, ByteBuffer buff, Peer cliID) {
			}

			@Override
			public Object fromArray(ByteBuffer buff, Peer originID) {
				return null;
			}
		});

		registerTypeConverter(Object.class, new TypeConverter() {

			@Override
			public void toArray(Object o, ByteBuffer buff, Peer cliID)
					throws IOException {
				buff.putObject(cliID);
				buff.putObject(o);
			}

			@Override
			public Object fromArray(ByteBuffer buff, Peer originID)
					throws Exception {
				Peer client = (Peer) buff.getObject();
				ByteArrayInputStream bis = new ByteArrayInputStream(buff
						.getByteArray());
				MarshallerInputStream stream = new MarshallerInputStream(bis,
						clp, client);
				Object ret = stream.readObject();
				stream.close();
				return ret;
			}
		});

		registerTypeConverter(MethodCall.class, new TypeConverter() {

			@Override
			public void toArray(Object o, ByteBuffer buff, Peer cliID)
					throws Exception {
				MethodCall mc = (MethodCall) o;
				buff.putString(mc.getName());
				buff.putString(mc.getObjectKey());

				Object[] objects = mc.getObjects();
				buff.putInt(objects.length);
				for (Object object : objects)
					objectToByteArray(object, buff, cliID);
			}

			@Override
			public Object fromArray(ByteBuffer buff, Peer originID)
					throws Exception {
				List<Object> objects = new ArrayList<>();
				String name = buff.getString();
				String k = buff.getString();
				int num = buff.getInt();
				for (int j = 0; j < num; j++)
					objects.add(getObjectFromArray(buff, originID));
				return new MethodCall(k, name, objects.toArray());
			}
		});

		registerTypeConverter(UUID.class, new TypeConverter() {

			@Override
			public void toArray(Object o, ByteBuffer ByteBuffer, Peer cliID) {
				ByteBuffer.putUUID((UUID) o);
			}

			@Override
			public Object fromArray(ByteBuffer buff, Peer originID) {
				return buff.getUUID();
			}
		});

		registerTypeConverter(byte[].class, new TypeConverter() {

			@Override
			public void toArray(Object o, ByteBuffer ByteBuffer, Peer cliID) {
				ByteBuffer.putByteArray((byte[]) o);
			}

			@Override
			public Object fromArray(ByteBuffer buff, Peer originID) {
				return buff.getByteArray();
			}
		});
	}

	public TypeConverters(ClassLoaderProvider cl) {
		this.clp = cl;
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

	public void objectToByteArray(Object o, ByteBuffer buffer, Peer client)
			throws Exception {
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
	}

	public Object getObjectFromArray(ByteBuffer buff, Peer originID)
			throws Exception {
		byte type = buff.get();
		String className = types.get(type);
		TypeConverter converter = convs.get(className);
		return converter.fromArray(buff, originID);
	}
}
