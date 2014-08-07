package edu.jlime.core.marshalling;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.log4j.Logger;

import edu.jlime.core.rpc.RPCDispatcher;

public class ClientClassLoader extends ClassLoader {

	HashMap<String, byte[]> classLoaderData = new HashMap<>();

	String clientID;

	RPCDispatcher disp;

	private Map<String, Class<?>> loaded = new ConcurrentHashMap<String, Class<?>>();

	Logger log = Logger.getLogger(ClientClassLoader.class);

	public ClientClassLoader(ClassLoader parent, String classSource,
			RPCDispatcher rpcDispatcher) {
		super(parent);
		this.clientID = classSource;
		this.disp = rpcDispatcher;
	}

	ConcurrentHashMap<String, Object> classesBeingObtained = new ConcurrentHashMap<>();

	@Override
	protected Class<?> findClass(String name) throws ClassNotFoundException {
		try {
			Class<?> alreadyLoaded = loaded.get(name);
			if (alreadyLoaded != null)
				return alreadyLoaded;
		} catch (Exception e) {
		}
		try {
			return Class.forName(name);
		} catch (Exception e) {
		}
		try {
			return super.findClass(name);
		} catch (Exception e) {
		}

		try {
			synchronized (this) {
				if (!classesBeingObtained.containsKey(name))
					classesBeingObtained.put(name, new Object());
			}

			Object lock = classesBeingObtained.get(name);
			synchronized (lock) {
				Class<?> c = loaded.get(name);
				if (c == null) {
					byte[] cl = disp.getClassFromSource(name, clientID);
					c = loadClassFromBytes(cl, name);
					loaded.put(name, c);
				}
				return c;
			}

		} catch (Exception e) {
			log.error("", e);
		}
		throw new ClassNotFoundException("ClassLoader couldn't find class "
				+ name);
	}

	public String getClientID() {
		return clientID;
	}

	public HashMap<String, byte[]> getData() {
		return classLoaderData;
	}

	public Class<?> loadClassFromBytes(byte[] bytes, String name)
			throws Exception {
		try {
			Class<?> c = defineClass(name, bytes, 0, bytes.length);
			resolveClass(c);
			classLoaderData.put(name, bytes);
			return c;
		} catch (LinkageError e) {
			// Maybe someone already resolved it.
			Class<?> cl = loaded.get(name);
			if (cl != null)
				return cl;
		}
		throw new ClassNotFoundException("Class " + name
				+ "  could not be loaded.");
	}

	public Class<?> getLoaded(String name) {
		return loaded.get(name);
	}

	public void setData(HashMap<String, byte[]> data) {
		this.classLoaderData.putAll(data);
	}

}
