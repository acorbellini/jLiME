package edu.jlime.jd;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TreeSet;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.Semaphore;
import java.util.concurrent.ThreadFactory;

import org.apache.log4j.Logger;

import edu.jlime.core.cluster.BroadcastException;
import edu.jlime.core.cluster.Cluster;
import edu.jlime.core.cluster.ClusterChangeListener;
import edu.jlime.core.cluster.Peer;
import edu.jlime.core.marshalling.TypeConverter;
import edu.jlime.core.marshalling.TypeConverters;
import edu.jlime.core.rpc.RPCDispatcher;
import edu.jlime.core.stream.RemoteInputStream;
import edu.jlime.core.stream.RemoteOutputStream;
import edu.jlime.jd.client.JobContextImpl;
import edu.jlime.jd.job.ResultManager;
import edu.jlime.jd.rpc.JobExecutor;
import edu.jlime.jd.rpc.JobExecutorBroadcast;
import edu.jlime.jd.rpc.JobExecutorFactory;
import edu.jlime.metrics.metric.Metrics;
import edu.jlime.util.ByteBuffer;

public class JobDispatcher implements ClusterChangeListener, JobExecutor {

	private static final String JOB_DISPATCHER = "JD";

	public static final String ISEXEC = "ISEXEC";

	public static final String TAGS = "TAGS";

	private static final int TIME_TO_SHOWUP = 20000;

	private ExecEnvironment env;

	private RPCDispatcher rpc;

	private Semaphore initLock = new Semaphore(0);

	private Set<Peer> executors = Collections
			.synchronizedSet(new TreeSet<Peer>());

	private HashMap<String, List<Peer>> byTag = new HashMap<>();

	private Map<UUID, ResultManager<?>> jobMap = new ConcurrentHashMap<UUID, ResultManager<?>>();

	private Logger log = Logger.getLogger(JobDispatcher.class);

	private int minServers;

	private StreamProvider streamer;

	private ExecutorService exec = Executors
			.newCachedThreadPool(new ThreadFactory() {
				@Override
				public Thread newThread(Runnable r) {
					Thread t = Executors.defaultThreadFactory().newThread(r);
					t.setName("JobDispatcher");
					return t;
				}
			});

	private List<CloseListener> closeListeners = new ArrayList<>();

	private JobExecutorFactory factory;

	private Metrics metrics;

	private Cluster cluster;

	private Map<String, Object> globals = new ConcurrentHashMap<>();

	public Map<UUID, ResultManager<?>> getJobMap() {
		return jobMap;
	}

	public JobDispatcher(int minPeers, RPCDispatcher rpc) {
		this.minServers = minPeers;
		this.env = new ExecEnvironment(this);
		this.rpc = rpc;
		this.cluster = rpc.getCluster();
		this.cluster.addChangeListener(this);

		rpc.registerTarget(JOB_DISPATCHER, this, true);

		factory = new JobExecutorFactory(rpc, JOB_DISPATCHER);

		final TypeConverters tc = rpc.getMarshaller().getTc();
		tc.registerTypeConverter(JobContainer.class, new TypeConverter() {
			@Override
			public void toArray(Object o, ByteBuffer buffer, Peer cliID)
					throws Exception {
				JobContainer jc = (JobContainer) o;
				tc.objectToByteArray(jc.getRequestor(), buffer, cliID);
				tc.objectToByteArray(jc.getJob(), buffer, cliID);
				buffer.putUUID(jc.getJobID());
				buffer.putBoolean(jc.isNoresponse());
			}

			@Override
			public Object fromArray(ByteBuffer buff) throws Exception {

				ClientNode p = (ClientNode) tc.getObjectFromArray(buff);

				ClientJob<?> job = (ClientJob<?>) tc.getObjectFromArray(buff);
				UUID id = buff.getUUID();
				boolean isNoResponse = buff.getBoolean();
				JobContainer jc = new JobContainer(job, p);
				jc.setID(id);
				jc.setNoResponse(isNoResponse);
				return jc;
			}
		});

		tc.registerTypeConverter(ClientNode.class, new TypeConverter() {
			@Override
			public void toArray(Object o, ByteBuffer buffer, Peer cliID)
					throws Exception {
				ClientNode jc = (ClientNode) o;
				tc.objectToByteArray(jc.getPeer(), buffer, cliID);
				tc.objectToByteArray(jc.getClient(), buffer, cliID);
			}

			@Override
			public Object fromArray(ByteBuffer buff) throws Exception {
				Peer p = (Peer) tc.getObjectFromArray(buff);
				Peer client = (Peer) tc.getObjectFromArray(buff);
				ClientNode jn = new ClientNode(p, client, JobDispatcher.this);
				return jn;
			}
		});

		tc.registerTypeConverter(RemoteReference.class, new TypeConverter() {
			@Override
			public void toArray(Object o, ByteBuffer buffer, Peer cliID)
					throws Exception {
				RemoteReference rr = (RemoteReference) o;
				tc.objectToByteArray(rr.getNode(), buffer, cliID);
				buffer.putString(rr.getKey());
			}

			@Override
			public Object fromArray(ByteBuffer buff) throws Exception {
				ClientNode p = (ClientNode) tc.getObjectFromArray(buff);
				String key = buff.getString();
				RemoteReference rr = new RemoteReference(p, key);
				return rr;
			}
		});

	}

	public void setStreamer(StreamProvider streamer) {
		this.streamer = streamer;
	}

	public void deleteAndStop(Peer srv) {
		env.remove(srv);
	}

	public <R> void mcastAsync(Collection<ClientNode> peers, ClientJob<R> j)
			throws Exception {
		List<Peer> copy = new ArrayList<>();
		for (ClientNode jobNode : peers) {
			copy.add(jobNode.getPeer());
		}
		if (!copy.isEmpty()) {
			JobContainer jw = new JobContainer(j, new ClientNode(
					getLocalPeer(), getLocalPeer(), this));
			jw.setNoResponse(true);

			JobExecutorBroadcast other = factory.getBroadcast(copy,
					j.getClient());
			other.execute(jw);
		}
	}

	public <R> Map<ClientNode, R> mcast(List<ClientNode> peers, ClientJob<R> j)
			throws Exception {
		if (log.isDebugEnabled())
			log.debug("Broadcasting job " + j + " to " + peers);
		BroadcastResultManager<R> rm = new BroadcastResultManager<R>(
				peers.size());
		if (log.isDebugEnabled())
			log.debug("Creating copy of peers.");

		List<Peer> copy = new ArrayList<>();
		for (ClientNode jobNode : peers) {
			copy.add(jobNode.getPeer());
		}

		Iterator<Peer> it = copy.iterator();
		UUID jobid = UUID.randomUUID();
		addJobMapping(rm, jobid, peers);

		if (log.isDebugEnabled())
			log.debug("Checking if it's local.");
		while (it.hasNext()) {
			Peer peer = it.next();
			JobContainer jw = new JobContainer(j, jobid, new ClientNode(
					getLocalPeer(), j.getClient(), this));
			try {
				JobExecutor localJD = DispatcherManager.getJD(peer);
				if (localJD != null) {
					if (log.isDebugEnabled())
						log.debug("Executing job on local JD (" + localJD + ")"
								+ j);
					it.remove();
					localJD.execute(jw);
				}
			} catch (Exception e) {
				rm.addException(peer, e);
			}
		}
		if (!copy.isEmpty()) {
			JobContainer jw = new JobContainer(j, jobid, new ClientNode(
					getLocalPeer(), j.getClient(), this));
			if (log.isDebugEnabled())
				log.debug("Creating JobExecutorBroadcast");
			JobExecutorBroadcast remote = factory.getBroadcast(copy,
					j.getClient());
			if (log.isDebugEnabled())
				log.debug("Calling JobExecutorBroadcast");
			try {
				remote.execute(jw);
			} catch (BroadcastException e) {
				for (Entry<Peer, Exception> el : e.getListOfExcep().entrySet()) {
					rm.addException(el.getKey(), el.getValue());
				}
			} catch (Exception e) {
				throw new Exception("Exception calling broadcast on " + copy, e);
			}
		}
		if (log.isDebugEnabled())
			log.debug("Waiting for results.");
		rm.waitResults();

		if (!rm.getException().isEmpty())
			throw rm.getException();
		return rm.getRes();

	}

	private void addJobMapping(ResultManager<?> rm, UUID jobID,
			List<ClientNode> peers) {
		jobMap.put(jobID, rm);
	}

	public void execAsync(final ClientNode dest, final ClientJob<?> j,
			final ResultManager<?> m) throws Exception {
		Peer cli = j.getClient();
		if (!cluster.contains(cli)) {
			log.error("Won't send job for " + j.getClient()
					+ ", a server that has crashed.");
			return;
		}
		JobContainer job = new JobContainer(j, new ClientNode(
				cluster.getLocalPeer(), cli, this));
		if (m != null) {
			ArrayList<ClientNode> al = new ArrayList<>();
			al.add(dest);
			addJobMapping(m, job.getJobID(), al);
			job.setNoResponse(false);
		} else
			job.setNoResponse(true);

		try {
			JobExecutor localJD = DispatcherManager.getJD(dest.getPeer());
			if (localJD != null) {
				if (log.isDebugEnabled())
					log.debug("Invoking LOCAL execute method for job "
							+ job.getJobID());
				localJD.execute(job);
			} else {
				if (log.isDebugEnabled())
					log.debug("Calling asynchronously \"execute\" method on server "
							+ dest
							+ ", Job ID "
							+ job.getJobID()
							+ " and type " + j.getClass());

				JobExecutor remote = factory.get(dest.getPeer(), job.getJob()
						.getClient());
				remote.execute(job);
			}
		} catch (Exception e) {
			log.error(e.getClass() + " " + e.getMessage());
			result(e, job.getJobID(), dest);
		}
	}

	public <R> Future<R> execAsyncWithFuture(final ClientNode address,
			final ClientJob<R> job) {
		Callable<R> task = new Callable<R>() {
			@Override
			public R call() throws Exception {
				return execSync(address, job);
			}
		};

		return exec.submit(task);
	}

	public <R> R execSync(ClientNode address, ClientJob<R> job)
			throws Exception {
		final Semaphore lock = new Semaphore(0);
		final ArrayList<R> finalRes = new ArrayList<>();
		final List<Exception> exceptionList = new ArrayList<>();
		execAsync(address, job, new ResultManager<R>() {
			@Override
			public void handleException(Exception res, String job,
					ClientNode peer) {
				exceptionList.add(res);
				lock.release();
			}

			@Override
			public void handleResult(R res, String job, ClientNode peer) {
				finalRes.add(res);
				lock.release();
			}
		});

		try {
			lock.acquire();
		} catch (InterruptedException e) {
			log.error("", e);
		}

		if (!exceptionList.isEmpty()) {
			throw exceptionList.get(0);
		}

		if (finalRes.isEmpty())
			throw new Exception("Final Res is empty.");
		return finalRes.get(0);
	}

	@Override
	public void execute(JobContainer j) throws Exception {
		if (!cluster.waitFor(j.getJob().getClient(), TIME_TO_SHOWUP)) {
			log.error("Won't execute a job for " + j.getJob().getClient()
					+ ", a client that has crashed.");
			throw new NotInClusterException();
		}

		if (log.isDebugEnabled())
			log.debug("Executing job " + j.getJobID() + " of client "
					+ j.getJob().getClient() + " and type "
					+ j.getJob().getClass() + " from " + j.getRequestor()
					+ " on " + getLocalPeer());
		try {
			j.setSrv(this);
			if (metrics != null)
				metrics.counter("jlime.jobs.in").count();
			Peer cli = j.getJob().getClient();
			if (cli == null)
				log.error("Client for job " + j.getJob().getClass()
						+ " is null, jobID:" + j.getJobID());
			JobContextImpl cliEnv = env.getClientEnv(cli);
			if (cliEnv == null)
				log.error("Client Environment not created for client " + cli);
			else {
				if (log.isDebugEnabled())
					log.debug("Submitting job " + j.getJobID() + " of client "
							+ j.getJob().getClient() + " from "
							+ j.getRequestor());
				cliEnv.execute(j);
			}
		} catch (Exception e) {
			throw e;
		}
	}

	public ClientCluster getCluster() {
		return new ClientCluster(this, getLocalPeer());
	}

	public ExecEnvironment getEnv() {
		return env;
	}

	public int getMinServers() {
		return minServers;
	}

	@Override
	public void result(final Object res, final UUID jobID, final ClientNode req)
			throws Exception {
		if (log.isDebugEnabled())
			log.debug("Processing result of job " + jobID + " from " + req);

		if (exec.isShutdown()) {
			log.info("Can't process result, JobDispatcher is closed.");
			return;
		}

		exec.execute(new Runnable() {
			@Override
			public void run() {
				try {
					ResultManager manager = jobMap.get(jobID);
					if (manager == null) {
						if (Exception.class.isAssignableFrom(res.getClass()))
							log.error("Received asynchronous exception from "
									+ req, (Exception) res);
						else
							log.info("Result was not expected from job "
									+ jobID + " from server " + req);
					} else {
						manager.manageResult(JobDispatcher.this, jobID, res,
								req);

						if (metrics != null)
							metrics.counter("jlime.jobs.out").count();

					}
					if (log.isDebugEnabled())
						log.debug("Leaving result method for job " + jobID);
				} catch (Exception e) {
					log.error("", e);
				}
			}
		});
	}

	public void sendResult(Object res, ClientNode req, UUID jobID, Peer cliID)
			throws Exception {
		JobExecutor localJD = DispatcherManager.getJD(req.getPeer());
		if (localJD != null) {
			if (log.isDebugEnabled())
				log.debug("Sending result for job " + jobID
						+ " to LOCAL dispatcher.");
			localJD.result(res, jobID, new ClientNode(getLocalPeer(), cliID,
					this));
		} else {
			if (log.isDebugEnabled())
				log.debug("Sending result for job " + jobID + " to " + req);

			JobExecutor remote = factory.get(req.getPeer(), null);
			remote.result(res, jobID, new ClientNode(getLocalPeer(), cliID,
					this));
		}
	}

	public void setMinServers(int minServers) {
		this.minServers = minServers;
	}

	public void start() throws Exception {
		DispatcherManager.registerJD(this);

		for (Peer p : cluster) {
			peerAdded(p, cluster);
		}
		rpc.start();
		if (executorsSize() < minServers)
			initLock.acquire();

	}

	public void stop() throws Exception {
		DispatcherManager.unregisterJD(this);
		rpc.stop();
		exec.shutdown();
		for (CloseListener cl : closeListeners) {
			cl.onStop();
		}

	}

	@Override
	public void peerRemoved(Peer p, Cluster c) {
		String[] tags = p.getData(TAGS).split(",");
		for (String tag : tags)
			byTag.get(tag).remove(p);
		Boolean isExec = Boolean.valueOf(p.getData(ISEXEC));
		if (isExec)
			executors.remove(p);

		deleteAndStop(p);
		checkSize();
	}

	@Override
	public void peerAdded(Peer p, Cluster c) {
		String[] tags = p.getData(TAGS).split(",");
		for (String tag : tags) {
			if (!byTag.containsKey(tag))
				byTag.put(tag, new ArrayList<Peer>());
			byTag.get(tag).add(p);
		}

		Boolean isExec = Boolean.valueOf(p.getData(ISEXEC));
		if (isExec)
			executors.add(p);
		checkSize();
	}

	public int executorsSize() {
		return executors.size();
	}

	private void checkSize() {
		if (minServers >= 0 && executorsSize() >= getMinServers()) {
			initLock.release();
		} else
			// ACA SE PUEDE AMPLIAR A ESPERAR POR CIERTOS TAGS A QUE APAREZCAN
			log.info("Still waiting for "
					+ (getMinServers() - executorsSize())
					+ ((getMinServers() - executorsSize()) != 1 ? " executors "
							: " executor") + " to show up. I am : "
					+ getLocalPeer());

	}

	public RemoteInputStream getInputStream(UUID streamID, ClientNode from) {
		return streamer.getInputStream(streamID, from.getPeer());
	}

	public RemoteOutputStream getOutputStream(UUID streamID, ClientNode from) {
		return streamer.getOutputStream(streamID, from.getPeer());
	}

	public void addCloseListener(CloseListener listener) {
		this.closeListeners.add(listener);
	}

	public Peer getLocalPeer() {
		return cluster.getLocalPeer();
	}

	public Set<Peer> getExecutors() {
		return executors;
	}

	public ArrayList<Peer> getPeers() {
		return cluster.getPeers();
	}

	public void removeMap(UUID jobID) {
		jobMap.remove(jobID);
	}

	public String printInfo() {
		return "Local Peer: " + cluster.getLocalPeer();
	}

	public void setMetrics(Metrics mgr) {
		this.metrics = mgr;
		this.rpc.setMetrics(mgr);
	}

	public Metrics getMetrics() {
		return Metrics.copyOf(metrics);
	}

	public void setGlobal(String k, Object v) {
		globals.put(k, v);
	}

	public Object getGlobal(String k) {
		return globals.get(k);
	}

}