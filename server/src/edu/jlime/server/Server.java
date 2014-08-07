package edu.jlime.server;

import java.io.File;
import java.io.FileInputStream;
import java.lang.reflect.Constructor;
import java.util.ArrayList;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Semaphore;

import org.apache.log4j.Logger;

import edu.jlime.jd.DEFExtension;
import edu.jlime.jd.JobDispatcher;
import edu.jlime.jd.JobDispatcherFactory;
import edu.jlime.metrics.jmx.MetricsJMX;
import edu.jlime.metrics.metric.Metrics;
import edu.jlime.metrics.sysinfo.InfoProvider;
import edu.jlime.metrics.sysinfo.SysInfoProvider;
import edu.jlime.util.CLI;
import edu.jlime.util.StringUtils;

public class Server {

	public static void main(String[] args) throws Exception {
		// System.in.read();
		CLI cli = new CLI();
		// Logger log = Logger.getLogger(DEFServer.class);
		// log.info(StringUtils.printDEFTitle());

		cli.param("instances", "i", "Number of DEF Instances", 1);
		cli.mult("network", "n", "Network Implementation", "def", new String[] {
				"def", "jgroups" });
		cli.param("config", "c", "Configuration File");
		cli.param("extensions", "e", "Extension list (comma-separated)");
		try {
			cli.parse(args);
		} catch (Exception e) {
			System.out.println(cli.getHelp(
					"Lightweight Distributed Execution Framework",
					"Alejandro Corbellini", 60));
			return;
		}

		int instances = cli.getInt("instances");
		String impl = cli.get("network");
		String config = cli.get("config");
		String ext = cli.get("extensions");

		Properties prop = null;
		if (config != null) {
			prop = new Properties();
			prop.load(new FileInputStream(new File(config)));
		}
		String[] tags = new String[] { "DefaultServer" };
		JobDispatcherFactory fact = null;
		if (impl.equals("def")) {
			Class<?> c = Class.forName("edu.jlime.rpc.JlimeFactory");
			Constructor<?> cons = c.getConstructors()[0];
			// fact = JobDispatcherFactory.getDEFFactory(0, tags, true, prop);
			fact = ((JobDispatcherFactory) cons
					.newInstance(0, tags, true, prop));
		} else if (impl.equals("jgroups")) {
			Class<?> c = Class.forName("edu.jlime.jgroups.JGroupsFactory");
			Constructor<?> cons = c.getConstructors()[0];
			fact = ((JobDispatcherFactory) cons.newInstance(
					new FileInputStream(new File(config)), 0, tags, true));
		}
		String[] extensions = new String[] {};
		if (ext != null)
			extensions = ext.split(",");

		Server.run(instances, fact, extensions);
	}

	private static ArrayList<Server> run(int n,
			final JobDispatcherFactory factory, final String[] extClasses) {

		final ArrayList<Server> ret = new ArrayList<>();
		final Semaphore waitInit = new Semaphore(-n + 1);
		ExecutorService svc = Executors.newCachedThreadPool();

		for (int i = 0; i < n; i++) {

			svc.execute(new Runnable() {

				@Override
				public void run() {
					Server svr = null;
					try {
						svr = Server.create(extClasses, factory);
					} catch (Exception e) {
						e.printStackTrace();
					}
					synchronized (ret) {
						ret.add(svr);
					}
					waitInit.release();
				}
			});

		}
		svc.shutdown();
		try {
			waitInit.acquire();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		return ret;
	}

	private Server(JobDispatcher jd) {
		this.jd = jd;
	}

	JobDispatcher jd;

	public static Server create(String[] extClasses,
			JobDispatcherFactory factory) throws Exception {

		ArrayList<DEFExtension> extensions = new ArrayList<DEFExtension>();
		for (String extClass : extClasses) {
			try {
				extensions.add((DEFExtension) Class.forName(extClass)
						.newInstance());
			} catch (InstantiationException e) {
				e.printStackTrace();
			} catch (IllegalAccessException e) {
				e.printStackTrace();
			} catch (ClassNotFoundException e) {
				e.printStackTrace();
			}
		}

		JobDispatcher jd = factory.getJD();
		jd.start();

		Metrics mgr = new Metrics();
		for (InfoProvider sysinfo : SysInfoProvider.get())
			sysinfo.load(mgr);
		new ClusterProvider(jd).load(mgr);
		jd.setMetrics(mgr);

		for (DEFExtension defExtension : extensions) {
			defExtension.start(jd.getCluster());
		}

		// Logger.getLogger(DEFServer.class).info(jd.printInfo());

		try {
			String[] info = new String[2];
			info[0] = mgr.get("sysinfo.os").toString();
			info[1] = mgr.get("jlime.interface").toString();
			Logger.getLogger(Server.class)
					.info(StringUtils.printDEFTitle(info));
		} catch (Exception e) {
			e.printStackTrace();
		}

		MetricsJMX jmx = new MetricsJMX(mgr);
		jmx.start();

		return new Server(jd);
	}

	public static Server jLiME() throws Exception {
		Class<?> c = Class.forName("edu.jlime.rpc.JlimeFactory");
		Constructor<?> cons = c.getConstructors()[0];
		JobDispatcherFactory fact = ((JobDispatcherFactory) cons.newInstance(0,
				new String[] { "DefaultServer" }, true, null));
		return Server.create(new String[] {}, fact);
	}

	public void stop() throws Exception {
		jd.stop();
	}
}
