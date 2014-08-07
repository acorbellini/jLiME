package edu.jlime.client;

import java.io.Closeable;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.lang.reflect.Constructor;
import java.util.Properties;

import edu.jlime.jd.JobCluster;
import edu.jlime.jd.JobDispatcher;
import edu.jlime.jd.JobDispatcherFactory;

public class Client implements Closeable {

	JobDispatcher jd;

	public Client(JobDispatcherFactory factory) throws Exception {
		jd = factory.getJD();
		jd.start();
	}

	public static Client build() throws Exception {
		return build(0);
	}

	public static Client build(int i) throws Exception {

		String config = System.getProperty("def.config");
		Properties prop = null;

		if (config != null) {
			prop = new Properties();
			prop.load(new FileInputStream(new File(config)));
		}
		Class<?> c = Class.forName("edu.jlime.rpc.JlimeFactory");
		Constructor<?> cons = c.getConstructors()[0];
		JobDispatcherFactory fact = ((JobDispatcherFactory) cons.newInstance(i,
				new String[] { "DefaultClient" }, false, prop));
		return new Client(fact);
	}

	public JobCluster getCluster() {
		return jd.getCluster();
	}

	@Override
	public void close() throws IOException {
		try {
			jd.stop();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	// public void close() throws Exception {
	// jd.stop();
	// }

	// public static void main(String[] args) throws NumberFormatException,
	// FileNotFoundException, Exception {
	// InputStream config = null;
	// if (args.length > 1)
	// config = new FileInputStream(new File(args[1]));
	//
	// if (config == null)
	// config = JGroupsFactory.getConfig();
	// Integer min = new Integer(args[0]);
	// String[] tags = new String[] { "DefaultClient" };
	//
	// JobDispatcherFactory factory = JobDispatcherFactory.getJGroupsFactory(
	// config, min, tags, false);
	//
	// new DEFClient(factory);
	// }
}
