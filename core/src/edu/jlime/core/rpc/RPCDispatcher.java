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
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.log4j.Logger;

import edu.jlime.core.cluster.BroadcastException;
import edu.jlime.core.cluster.Cluster;
import edu.jlime.core.cluster.Peer;
import edu.jlime.core.cluster.PeerFilter;
import edu.jlime.core.marshalling.ClientClassLoader;
import edu.jlime.core.marshalling.Marshaller;
import edu.jlime.core.marshalling.PeerClassLoader;
import edu.jlime.core.transport.Address;
import edu.jlime.core.transport.Streamer;
import edu.jlime.core.transport.Transport;
import edu.jlime.metrics.metric.Metrics;
import edu.jlime.util.StreamUtils;

public class RPCDispatcher implements TransportListener {

	public static enum RPCStatus {
		STARTED, INIT, DOWN
	}

	private static final Map<Peer, RPCDispatcher> localdispatchers = new ConcurrentHashMap<>();

	private static final String RPC = "RPC";

	private PeerClassLoader cl = new PeerClassLoader();

	private Transport tr;

	private Peer localPeer;

	private Marshaller marshaller;

	private Logger log = Logger.getLogger(RPCDispatcher.class);

	private Map<String, Object> targets = new ConcurrentHashMap<>();

	private Map<String, RPCStatus> targetsStatuses = new ConcurrentHashMap<>();

	private Map<String, Method[]> targetsMethods = new ConcurrentHashMap<>();

	// private Map<Method, Class<?>[]> targetsMethodsTypes = new
	// ConcurrentHashMap<>();

	private Metrics metrics;

	// private ExecutorService asyncExec = Executors
	// .newCachedThreadPool(new ThreadFactory() {
	//
	// @Override
	// public Thread newThread(Runnable r) {
	// Thread t = Executors.defaultThreadFactory().newThread(r);
	// t.setName("RPCAsyncThreads");
	// return t;
	// }
	// });

	private ExecutorService broadcastExec = Executors
			.newCachedThreadPool(new ThreadFactory() {
				@Override
				public Thread newThread(Runnable r) {
					Thread t = Executors.defaultThreadFactory().newThread(r);
					t.setName("RPCDispatcherBroadcast");
					return t;
				}
			});

	private volatile boolean stopped = false;

	public RPCDispatcher(Transport tr) {
		this.tr = tr;
		this.marshaller = new Marshaller(this);
		this.registerTarget(RPC, this, true);
		this.tr.listen(this);
		localPeer = tr.getCluster().getLocalPeer();
		localdispatchers.put(localPeer, this);
	}

	public Object callSync(Peer dest, Peer clientID, MethodCall call)
			throws Exception {
		return call(dest, clientID, call, true);
	}

	public void callAsync(final Peer dest, final Peer clientID,
			final MethodCall call) throws Exception {
		// if (asyncExec.isShutdown()) {
		// log.warn("Async Executor is shutted down, maybe the rpc dispatcher was closed.");
		// return;
		// }
		//
		// asyncExec.execute(new Runnable() {
		// @Override
		// public void run() {
		// try {
		call(dest, clientID, call, false);
		// } catch (Exception e) {
		// e.printStackTrace();
		// }
		// }
		// });

	}

	private Object call(Peer dest, Peer clientID, MethodCall call, boolean sync)
			throws Exception {
		// Local call
		RPCDispatcher local = localdispatchers.get(dest);
		if (local != null) {
			if (log.isDebugEnabled())
				log.debug("Dispatching methodcall to local target.");
			return local.callTarget(call);
		}

		byte[] marshalled = getMarshaller().toByteArray(clientID, call);

		try {
			if (sync) {
				if (log.isDebugEnabled())
					log.debug("Dispatching SYNC call " + call + " to " + dest);

				byte[] sendSync = tr.sendSync(dest, marshalled);

				Object object = getMarshaller().getObject(sendSync, dest);

				if (object instanceof Exception)
					throw (Exception) object;
				return object;
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

	public void setMarshaller(Marshaller marshaller) {
		this.marshaller = marshaller;
	}

	public void stop() throws Exception {
		// asyncExec.shutdown();
		targets.clear();
		targetsMethods.clear();
		targetsStatuses.clear();
		
		this.marshaller.clear();
		
		this.stopped = true;
		broadcastExec.shutdown();
		tr.stop();
		localdispatchers.remove(localPeer);
	};

	protected Object callTarget(MethodCall mc) throws Exception {
		try {
			Object target = getTarget(mc.getObjectKey());
			Class<?> objClass = target.getClass();
			Method m = findMethod(objClass, mc,
					targetsMethods.get(mc.getObjectKey()));
			return m.invoke(target, mc.getObjects());
		} catch (Exception e) {
			throw new Exception("Error calling " + mc + " ", e);
		}
	}

	private boolean checkParams(MethodCall mc, Method m) throws Exception {
		// Class<?>[] types = targetsMethodsTypes.get(m) m.gett;
		Object[] objects = mc.getObjects();
		Class<?>[] types = m.getParameterTypes();
		Class<?>[] searchedTypes = mc.getArgTypes();
		if (m.getName().equals(mc.getName())
				&& types.length == searchedTypes.length) {
			for (int i = 0; i < types.length; i++) {
				if (!types[i].isAssignableFrom(searchedTypes[i])
						&& objects[i] != null)
					if (Wrappers.get(types[i]) != null
							&& Wrappers.get(types[i]).isAssignableFrom(
									searchedTypes[i]))
						// mc.unwrapArgument(i);
						;
					else
						return false;
			}
			return true;
		} else
			return false;

	}

	private Method findMethod(Class<?> objClass, MethodCall mc, Method[] methods)
			throws Exception {
		for (Method m : methods) {
			if (checkParams(mc, m))
				return m;
		}
		throw new NoSuchMethodException("Method : " + mc.getName()
				+ ". Object class: " + objClass.toString() + ".");
	}

	public void multiCallAsync(final List<Peer> peers, final Peer client,
			final String target, final String method, final Object[] objects)
			throws Exception {
		// asyncExec.execute(new Runnable() {
		// @Override
		// public void run() {
		// try {
		multiCall(peers, client, new MethodCall(target, method, objects));
		// } catch (Exception e) {
		// log.error("Error executing asynchronous multiCall", e);
		// }
		// }
		// });
	}

	public <T> Map<Peer, T> multiCall(List<Peer> peers, Peer client,
			String target, String method, Object[] objects) throws Exception {
		return multiCall(peers, client, new MethodCall(target, method, objects));
	}

	private <T> Map<Peer, T> multiCall(List<Peer> peers, final Peer client,
			final MethodCall call) throws Exception {
		// Because I want to marshall this when needed (if this array were a
		// byte[] and final, I wouldn't be able to create it on demand)
		final byte[][] marshalled = new byte[1][];

		final ReentrantLock marshalledLock = new ReentrantLock();

		if (log.isDebugEnabled())
			log.debug("Broadcasting " + call + " to " + peers);
		final Semaphore sem = new Semaphore(-peers.size() + 1);

		final BroadcastException[] exception = new BroadcastException[1];

		final Map<Peer, T> ret = new ConcurrentHashMap<>();
		for (final Peer p : peers) {
			broadcastExec.execute(new Runnable() {

				@SuppressWarnings("unchecked")
				@Override
				public void run() {
					try {
						if (log.isDebugEnabled())
							log.debug("Sending broadcast message synchronously to "
									+ p);
						RPCDispatcher local = localdispatchers.get(p);
						T sendSync = null;
						if (local != null)
							sendSync = (T) local.callTarget(call);
						else {
							if (marshalled[0] == null) {
								marshalledLock.lock();
								if (marshalled[0] == null)
									marshalled[0] = getMarshaller()
											.toByteArray(client, call);
								marshalledLock.unlock();
							}
							byte[] res = tr.sendSync(p, marshalled[0]);
							sendSync = (T) getMarshaller().getObject(res, p);
						}
						if (sendSync != null)
							ret.put(p, sendSync);
						// else
						// ret.put(p, new Object());
						if (log.isDebugEnabled())
							log.debug("Finished sending broadcast message synchronously to "
									+ p);
					} catch (Exception e) {
						synchronized (exception) {
							if (exception[0] == null)
								exception[0] = new BroadcastException(
										"Broadcast Exception");
						}
						exception[0].put(p, e);
						log.error("Error making broadcast rpc to " + p, e);
					}
					sem.release();
				}
			});
		}
		while (!sem.tryAcquire(5, TimeUnit.SECONDS)) {
			if (log.isDebugEnabled())
				log.debug("Waiting for semaphore in multiCall Permits:"
						+ sem.availablePermits() + " , call:  " + call
						+ " , peers : " + peers);
		}
		if (log.isDebugEnabled())
			log.debug("FINISHED broadcasting " + call + " bytes to " + peers);
		if (exception[0] != null && !exception[0].isEmpty())
			throw exception[0];
		return ret;
	};

	public Object callSync(Peer dest, Peer clientID, String objectKey,
			String method, Object[] args) throws Exception {
		return callSync(dest, clientID, new MethodCall(objectKey, method, args));
	}

	public void callAsync(Peer addr, Peer clientID, String objectKey,
			String method, Object[] args) throws Exception {
		callAsync(addr, clientID, new MethodCall(objectKey, method, args));
	}

	public Marshaller getMarshaller() {
		return marshaller;
	}

	public byte[] getClassDefinition(String n) throws ClassNotFoundException {
		String rep = n.replace(".", "/") + ".class";
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

	public byte[] getClassFromSource(String name, Peer clientID)
			throws Exception {
		byte[] array = (byte[]) callSync(clientID, null, RPC,
				"getClassDefinition", new String[] { name });
		// return Compression.uncompress(array);
		return array;
	}

	public HashMap<String, byte[]> getClassLoaderData(Peer cliID) {
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

	public Class<?> loadClass(Peer classSource, String name)
			throws ClassNotFoundException {
		if (classSource == null)
			return this.getClass().getClassLoader().loadClass(name);

		ClientClassLoader loader = cl.getCL(classSource);
		if (loader == null) {
			loader = new ClientClassLoader(RPCDispatcher.class.getClassLoader()
					.getParent(), classSource, this);
			cl.add(classSource, loader);
		}
		return loader.loadClass(name);
	}

	public void registerTarget(String key, Object target, boolean replace) {
		registerTarget(key, target, replace, RPCStatus.STARTED);

	}

	public void registerTarget(String key, Object target, boolean replace,
			RPCStatus status) {
		if (!replace && targets.containsKey(key))
			return;
		// System.out.println("Putting target " + key + ": " + target);

		targets.put(key, target);
		targetsStatuses.put(key, status);
		Method[] methods = target.getClass().getMethods();
		targetsMethods.put(key, methods);

		synchronized (targetsStatuses) {
			targetsStatuses.notifyAll();
		}
		// for (Method method : methods)
		// targetsMethodsTypes.put(method, method.getParameterTypes());

	}

	@Override
	public byte[] process(Address origin, byte[] buff) {
		try {
			Peer peer = getCluster().getByAddress(origin);
			Object obj = getMarshaller().getObject(buff, peer);
			// if (log.isDebugEnabled())
			// log.debug("Finished unmarshalling data received");
			if (Exception.class.isAssignableFrom(obj.getClass())) {
				AsyncLogger.logAsyncException((Exception) obj);
				return null;
			}
			MethodCall mc = (MethodCall) obj;
			// if (log.isDebugEnabled())
			// log.debug("Received method call " + mc.getName() + " from "
			// + getCluster().getByAddress(origin) + ", invoking.");

			Object callTarget = callTarget(mc);

			byte[] byteArray = getMarshaller().toByteArray(callTarget);
			// if (log.isDebugEnabled())
			// log.debug("Returning from method call " + mc.getName()
			// + " from " + getCluster().getByAddress(origin)
			// + ", invoking.");
			return byteArray;
		} catch (Exception e) {
			log.error(e, e);
			try {
				return getMarshaller().toByteArray(e);
			} catch (Exception e1) {
				e1.printStackTrace();
				return null;
			}
		}
	}

	AtomicBoolean started = new AtomicBoolean(false);

	public void start() throws Exception {
		if (started.compareAndSet(false, true))
			tr.start();
	}

	public void setMetrics(Metrics mgr) {
		this.metrics = mgr;
		this.tr.setMetrics(mgr);
	}

	public Cluster getCluster() {
		return tr.getCluster();
	}

	public Streamer getStreamer() {
		return tr.getStreamer();
	}

	// public <T, B> ClientManager<T, B> manage(ClientFactory<T, B> factory,
	// PeerFilter filter) {
	// return this.manage(factory, filter, null);
	// }

	public <T, B> ClientManager<T, B> manage(ClientFactory<T, B> factory,
			PeerFilter filter, Peer client) {
		return new ClientManager<T, B>(this, factory, filter, client);
	}

	public <T, B> ClientManager<T, B> manage(ClientFactory<T, B> f, Peer cli) {
		return this.manage(f, new PeerFilter() {

			@Override
			public boolean verify(Peer p) {
				return true;
			}
		}, cli);
	}

	public void register(Peer p, String name, Object pregelGraphLocal)
			throws Exception {
		callSync(p, localPeer, new MethodCall(RPC, "registerTarget",
				new Object[] { name, pregelGraphLocal, true }));
	}

	public void setTargetsStatuses(String k, RPCStatus newStat) {
		this.targetsStatuses.put(k, newStat);
		synchronized (targetsStatuses) {
			targetsStatuses.notifyAll();
		}
	}

	public Object getTarget(String name) throws Exception {
		RPCStatus rpcStatus = targetsStatuses.get(name);
		if (rpcStatus == null || !rpcStatus.equals(RPCStatus.STARTED))
			synchronized (targetsStatuses) {
				rpcStatus = targetsStatuses.get(name);
				while (!stopped
						&& (rpcStatus == null || !rpcStatus
								.equals(RPCStatus.STARTED)))
					try {
						if (log.isDebugEnabled())
							log.debug("Waiting for target " + name
									+ " to start.");
						targetsStatuses.wait(1000);
						rpcStatus = targetsStatuses.get(name);
					} catch (InterruptedException e) {
						e.printStackTrace();
					}
			}
		if (stopped)
			throw new Exception("RPC was stopped");
		return targets.get(name);
	}

	public static RPCDispatcher getLocalDispatcher(Peer dest) {
		return localdispatchers.get(dest);
	}

	public void registerIfAbsent(Peer p, String name, Object pregelGraphLocal)
			throws Exception {
		callSync(p, localPeer, new MethodCall(RPC, "registerTarget",
				new Object[] { name, pregelGraphLocal, false }));
	}

	public Object getRealAddress() {
		return tr.getRealAddress();
	}

	public Transport getTransport() {
		return tr;
	}

	public void unregisterTarget(String key, Object obj) {
		targets.remove(key);
		targetsStatuses.remove(key);
		targetsMethods.remove(key);
	}
}