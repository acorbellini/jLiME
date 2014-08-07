package edu.jlime.core.marshalling;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.UUID;

import javax.lang.model.type.NullType;

import edu.jlime.core.rpc.MethodCall;
import edu.jlime.util.ByteBuffer;

public class TypeConverters {

	private final String GENERIC = "GENERIC";

	private HashMap<String, ObjectConverter> convs = new HashMap<>();

	private HashMap<String, Byte> ids = new HashMap<>();

	private HashMap<Byte, String> types = new HashMap<>();

	private byte count = 0;

	private ClassLoaderProvider clp;

	{
		registerTypeConverter(Integer.class.getName(), new ObjectConverter() {

			@Override
			public void toArray(Object o, ByteBuffer buffer) {
				buffer.putInt((Integer) o);
			}

			@Override
			public Object fromArray(ByteBuffer buff, String originID,
					String cliID) {
				return buff.getInt();
			}
		});

		registerTypeConverter(Boolean.class.getName(), new ObjectConverter() {

			@Override
			public void toArray(Object o, ByteBuffer buff) {
				buff.putBoolean((Boolean) o);
			}

			@Override
			public Object fromArray(ByteBuffer buff, String originID,
					String cliID) {
				return buff.getBoolean();
			}
		});

		registerTypeConverter(String.class.getName(), new ObjectConverter() {

			@Override
			public void toArray(Object o, ByteBuffer buff) {
				buff.putString((String) o);
			}

			@Override
			public Object fromArray(ByteBuffer buff, String originID,
					String cliID) {
				return buff.getString();
			}
		});

		registerTypeConverter(NullType.class.getName(), new ObjectConverter() {

			@Override
			public void toArray(Object o, ByteBuffer buff) {
			}

			@Override
			public Object fromArray(ByteBuffer buff, String originID,
					String cliID) {
				return null;
			}
		});

		registerTypeConverter(GENERIC, new ObjectConverter() {

			@Override
			public void toArray(Object o, ByteBuffer buff) throws IOException {
				buff.putObject(o);
			}

			@Override
			public Object fromArray(ByteBuffer buff, String originID,
					String clientID) throws Exception {

				ByteArrayInputStream bis = new ByteArrayInputStream(buff
						.getByteArray());

				// JBossObjectInputStream stream = new
				// JBossObjectInputStream(bis);
				//
				// ClientClassLoader cs = disp.getClassSource(clientID);
				// if (cs != null)
				// stream.setClassLoader(cs);

				DEFMarshallerInputStream stream = new DEFMarshallerInputStream(
						bis, clp, clientID);
				Object ret = stream.readObject();
				stream.close();
				return ret;
			}
		});

		registerTypeConverter(MethodCall.class.getName(),
				new ObjectConverter() {

					@Override
					public void toArray(Object o, ByteBuffer buff)
							throws Exception {
						MethodCall mc = (MethodCall) o;

						buff.putString(mc.getName());
						buff.putString(mc.getObjectKey());

						Object[] objects = mc.getObjects();
						buff.putInt(objects.length);
						for (Object object : objects)
							objectToByteArray(object, buff);
					}

					@Override
					public Object fromArray(ByteBuffer buff, String originID,
							String clientID) throws Exception {
						List<Object> objects = new ArrayList<>();
						String name = buff.getString();
						String k = buff.getString();
						int num = buff.getInt();
						for (int j = 0; j < num; j++)
							objects.add(getObjectFromArray(buff, originID,
									clientID));
						return new MethodCall(k, name, objects.toArray());
					}
				});

		registerTypeConverter(UUID.class.getName(), new ObjectConverter() {

			@Override
			public void toArray(Object o, ByteBuffer buffer) {
				buffer.putUUID((UUID) o);
			}

			@Override
			public Object fromArray(ByteBuffer buff, String originID,
					String cliID) {
				return buff.getUUID();
			}
		});

		registerTypeConverter(byte[].class.getName(), new ObjectConverter() {

			@Override
			public void toArray(Object o, ByteBuffer buffer) {
				buffer.putByteArray((byte[]) o);
			}

			@Override
			public Object fromArray(ByteBuffer buff, String originID,
					String cliID) {
				return buff.getByteArray();
			}
		});
	}

	public TypeConverters(ClassLoaderProvider cl) {
		this.clp = cl;
	}

	public void registerTypeConverter(String className, ObjectConverter conv) {
		byte id = count++;
		ids.put(className, id);
		types.put(id, className);
		convs.put(className, conv);
	}

	public void objectToByteArray(Object o, ByteBuffer buffer) throws Exception {
		String classOfObject = o == null ? NullType.class.getName() : o
				.getClass().toString();
		// Default converter
		ObjectConverter converter = convs.get(GENERIC);
		byte type = ids.get(GENERIC);
		// Check if the converter is loaded
		if (convs.containsKey(classOfObject)) {
			converter = convs.get(classOfObject);
			type = ids.get(classOfObject);
		}
		buffer.put(type);
		converter.toArray(o, buffer);
	}

	public Object getObjectFromArray(ByteBuffer buff, String originID,
			String clientID) throws Exception {
		byte type = buff.get();
		String className = types.get(type);
		ObjectConverter converter = convs.get(className);
		return converter.fromArray(buff, originID, clientID);
	}
}
