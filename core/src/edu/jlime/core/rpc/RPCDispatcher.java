package edu.jlime.core.rpc;

import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Semaphore;
import java.util.concurrent.ThreadFactory;

import org.apache.log4j.Logger;

import edu.jlime.core.cluster.Cluster;
import edu.jlime.core.cluster.Peer;
import edu.jlime.core.marshalling.ClassLoaderProvider;
import edu.jlime.core.marshalling.ClientClassLoader;
import edu.jlime.core.marshalling.Marshaller;
import edu.jlime.core.marshalling.PeerClassLoader;
import edu.jlime.metrics.metric.Metrics;
import edu.jlime.util.StreamUtils;

public class RPCDispatcher implements ClassLoaderProvider, DataReceiver {

	private static final String RPC = "RPC";

	private PeerClassLoader cl = new PeerClassLoader();

	private Transport tr;

	private ExecutorService broadcastExec = Executors
			.newCachedThreadPool(new ThreadFactory() {

				@Override
				public Thread newThread(Runnable r) {
					Thread t = Executors.defaultThreadFactory().newThread(r);
					t.setName("RPCDispatcherBroadcast");
					return t;
				}
			});

	private Marshaller marshaller;

	private Logger log = Logger.getLogger(RPCDispatcher.class);

	private Cluster cluster;

	private Map<String, Object> targets = new ConcurrentHashMap<>();

	private Metrics metrics;

	public RPCDispatcher(Cluster c) {
		this.cluster = c;
		this.marshaller = new Marshaller(this);
		this.registerTarget(RPC, this);
	}

	public void setTransport(Transport tr) {
		this.tr = tr;
		this.tr.registerReceiver(this);
		if (metrics != null)
			this.tr.setMetrics(metrics);
	}

	public Object callSync(Peer dest, String clientID, MethodCall call)
			throws Exception {
		return call(dest, clientID, call, true);
	}

	public void callAsync(Peer dest, String clientID, MethodCall call)
			throws Exception {
		call(dest, clientID, call, false);
	}

	private Object call(Peer dest, String clientID, MethodCall call,
			boolean sync) throws Exception {
		if (dest.equals(cluster.getLocalPeer())) {
			if (log.isDebugEnabled())
				log.debug("Dispatching methodcall to local target.");
			Object obj = callTarget(call);
			return obj;
		}

		byte[] marshalled = getMarshaller().toByteArray(call, clientID);

		try {
			if (sync) {
				if (log.isDebugEnabled())
					log.debug("Dispatching SYNC call to " + dest);
				return getMarshaller().getObject(tr.sendSync(dest, marshalled),
						dest.getID());
			} else {
				if (log.isDebugEnabled())
					log.debug("Dispatching ASYNC call to " + dest);
				tr.sendAsync(dest, marshalled);
				return null;
			}
		} catch (Exception e) {
			throw new Exception("Method call " + call.getName() + " to " + dest
					+ " failed.", e);
		}
	}

	public void setMarshaller(Marshaller defMarshaller) {
		this.marshaller = defMarshaller;
	}

	public void stop() {
		broadcastExec.shutdown();
		tr.stop();
	};

	protected Object callTarget(MethodCall mc) throws Exception {
		Object target = targets.get(mc.getObjectKey());
		Class<?> objClass = target.getClass();
		Method m = findMethod(objClass, mc);
		return m.invoke(target, mc.getObjects());
	}

	private boolean checkParams(MethodCall mc, Method m) throws Exception {
		Class<?>[] types = m.getParameterTypes();
		List<Class<?>> searchedTypes = mc.getArgTypes();
		if (m.getName().equals(mc.getName())
				&& types.length == searchedTypes.size()) {
			for (int i = 0; i < types.length; i++) {
				if (!types[i].isAssignableFrom(searchedTypes.get(i)))
					return false;
			}
			return true;
		} else
			return false;

	}

	private Method findMethod(Class<?> objClass, MethodCall mc)
			throws Exception {
		for (Method m : objClass.getMethods()) {
			if (checkParams(mc, m))
				return m;
		}
		throw new NoSuchMethodException("Method : " + mc.getName()
				+ ". Object class: " + objClass.toString() + ".");
	}

	public void multiCallAsync(List<Peer> peers, String client, String target,
			String method, Object[] objects) throws Exception {
		multiCall(peers, client, new MethodCall(target, method, objects));
	}

	public Map<Peer, Object> multiCall(List<Peer> peers, String client,
			String target, String method, Object[] objects) throws Exception {
		return multiCall(peers, client, new MethodCall(target, method, objects));
	}

	private Map<Peer, Object> multiCall(List<Peer> peers, String client,
			MethodCall call) throws Exception {
		byte[] marshalled = getMarshaller().toByteArray(call, client);
		return broadcast(peers, marshalled);
	}

	public Map<Peer, Object> broadcast(List<Peer> peers, final byte[] marshalled)
			throws Exception {
		if (log.isDebugEnabled())
			log.debug("Broadcasting " + marshalled.length + " bytes to "
					+ peers);
		final Semaphore sem = new Semaphore(-peers.size() + 1);
		final Map<Peer, Object> ret = new HashMap<>();
		for (final Peer p : peers) {
			broadcastExec.execute(new Runnable() {
				@Override
				public void run() {
					try {
						if (log.isDebugEnabled())
							log.debug("Sending broadcast message synchronously to "
									+ p);
						ret.put(p,
								getMarshaller().getObject(
										tr.sendSync(p, marshalled), p.getID()));
						if (log.isDebugEnabled())
							log.debug("Finished sending broadcast message synchronously to "
									+ p);
					} catch (Exception e) {
						ret.put(p, e);
						e.printStackTrace();
					}
					sem.release();
				}
			});
		}
		sem.acquire();
		if (log.isDebugEnabled())
			log.debug("FINISHED broadcasting " + marshalled.length
					+ " bytes to " + peers);
		return ret;
	};

	public Object callSync(Peer dest, String clientID, String objectKey,
			String method, Object[] args) throws Exception {
		return callSync(dest, clientID, new MethodCall(objectKey, method, args));
	}

	public void callAsync(Peer addr, String clientID, String objectKey,
			String method, Object[] args) throws Exception {
		callAsync(addr, clientID, new MethodCall(objectKey, method, args));
	}

	public Marshaller getMarshaller() {
		return marshaller;
	}

	public byte[] getClassDefinition(String n) throws ClassNotFoundException {
		String rep = n.replace(".", "/") + ".class";
		// System.out.println("Requesting " + rep);
		InputStream is = null;
		if (is == null)
			try {
				is = ClassLoader.getSystemResourceAsStream(rep);
			} catch (Exception e) {
			}

		if (is == null)
			try {
				is = Class.forName(n).getProtectionDomain().getCodeSource()
						.getLocation().openStream();
			} catch (Exception e) {
			}

		if (is == null)
			throw new ClassNotFoundException(n);

		byte[] serialized = null;
		try {
			BufferedInputStream bis = new BufferedInputStream(is);
			byte[] data = StreamUtils.readFully(bis);
			
			serialized = data;// Compression.compress(buffer.build());
		} catch (IOException e) {
			log.error("Error Compressing class file", e);
		}
		return serialized;
	}

	public byte[] getClassFromSource(String name, String clientID)
			throws Exception {
		byte[] array = (byte[]) callSync(cluster.getByID(clientID), null, RPC,
				"getClassDefinition", new String[] { name });
		// return Compression.uncompress(array);
		return array;
	}

	public HashMap<String, byte[]> getClassLoaderData(String cliID) {
		ClientClassLoader cliCL = cl.getCL(cliID);
		if (cliCL == null)
			return new HashMap<>();
		return cliCL.getData();
	}

	@SuppressWarnings("unchecked")
	public HashMap<String, byte[]> getClassLoaderDataFromServer(
			String classSource, Peer origin) throws Exception {
		return (HashMap<String, byte[]>) callSync(origin, null, RPC,
				"getClassLoaderData", new Object[] { classSource });
	}

	@Override
	public Class<?> loadClass(String classSource, String name)
			throws ClassNotFoundException {
		if (classSource == null)
			return null;

		ClientClassLoader loader = cl.getCL(classSource);
		if (loader == null) {
			loader = new ClientClassLoader(RPCDispatcher.class.getClassLoader()
					.getParent(), classSource, this);
			cl.add(classSource, loader);
		}
		return loader.loadClass(name);
	}

	public void registerTarget(String key, Object target) {
		targets.put(key, target);
	}

	@Override
	public byte[] process(String origin, byte[] buff) {
		try {
			Object obj = getMarshaller().getObject(buff, origin.toString());
			if (log.isDebugEnabled())
				log.debug("Finished unmarshalling data received");
			if (Exception.class.isAssignableFrom(obj.getClass())) {
				AsyncLogger.logAsyncException((Exception) obj);
				return null;
			}
			MethodCall mc = (MethodCall) obj;
			if (log.isDebugEnabled())
				log.info("Received method call " + mc.getName() + " from "
						+ cluster.getByID(origin) + ", invoking.");

			return getMarshaller().toByteArray(callTarget(mc));
		} catch (Exception e) {
			log.error(e, e);
			try {
				return getMarshaller().toByteArray(new Object[] { e });
			} catch (Exception e1) {
				e1.printStackTrace();
				return null;
			}
		}
	}

	public void start() throws Exception {
		tr.start();
	}

	public void setMetrics(Metrics mgr) {
		this.metrics = mgr;
		this.tr.setMetrics(mgr);
	}
}