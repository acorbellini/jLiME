package edu.jlime.jd.client;

import java.io.Closeable;
import java.io.IOException;
import java.util.HashSet;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;

import org.apache.log4j.Logger;

import edu.jlime.core.cluster.Peer;
import edu.jlime.jd.ClientCluster;
import edu.jlime.jd.JobContainer;
import edu.jlime.jd.JobDispatcher;

public class JobContextImpl implements JobContext {

	private HashSet<Closeable> closeables = new HashSet<>();

	private ConcurrentHashMap<String, Object> data = new ConcurrentHashMap<>();

	private ExecutorService exec;

	private Peer id;

	private ClientCluster cluster;

	private JobDispatcher srv;

	public JobContextImpl(JobDispatcher srv, ClientCluster c, Peer cliId) {
		this.srv = srv;
		this.id = cliId;
		this.cluster = c;
		exec = Executors.newCachedThreadPool(new ThreadFactory() {
			@Override
			public Thread newThread(Runnable r) {
				Thread t = Executors.defaultThreadFactory().newThread(r);
				t.setName("JobContext for peer" + id);
				return t;
			}
		});
	}

	@Override
	public boolean contains(String map) {
		return data.containsKey(map);
	}

	public void execute(JobContainer j) throws Exception {
		exec.execute(j);
	}

	@Override
	public Object get(String prop) {
		return data.get(prop);
	}

	@Override
	public ClientCluster getCluster() {
		return cluster;
	}

	@Override
	public synchronized void put(String id, Object val) {
		if (Closeable.class.isAssignableFrom(val.getClass())) {
			closeables.add((Closeable) val);
		}
		data.put(id, val);
		notifyAll();
	}

	@Override
	public synchronized void putIfAbsent(String hashName, Object val) {
		if (!data.containsKey(hashName))
			put(hashName, val);
	}

	@Override
	public synchronized Object remove(String prop) {
		Object o = data.remove(prop);
		if (o instanceof Closeable)
			closeables.remove((Closeable) o);
		return o;
	}

	@Override
	public synchronized void stop() {
		for (Closeable c : closeables) {
			try {
				c.close();
			} catch (IOException e) {
				Logger.getLogger(JobContextImpl.class).error("", e);
			}
		}
		data.clear();
		closeables.clear();
		exec.shutdown();
	}

	@Override
	public String toString() {
		return "JobContext Keyset : " + data.keySet();
	}

	@Override
	public Object waitFor(String id) {
		Object obj = get(id);
		synchronized (this) {
			while (obj == null) {
				try {
					wait();
					obj = get(id);
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
			}
		}
		return obj;
	}

	@Override
	public Object getGlobal(String k) {
		return srv.getGlobal(k);
	}
}
