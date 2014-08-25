package edu.jlime.collections.intintarray.loader;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.Arrays;
import java.util.Properties;

import org.apache.log4j.Logger;

import edu.jlime.collections.intintarray.client.BundlerClient;
import edu.jlime.collections.intintarray.client.PersistentIntIntArrayMap;
import edu.jlime.collections.intintarray.client.jobs.StoreConfig;
import edu.jlime.collections.intintarray.db.StoreFactory.StoreType;
import edu.jlime.jd.client.Client;
import edu.jlime.util.RingQueue;
import gnu.trove.list.array.TIntArrayList;

public class Loader {

	private PersistentIntIntArrayMap client = null;

	private String path;

	private Integer empezarPorClave;

	private Boolean prefix;

	private Client jlime;

	public Loader(String propFilePath) {
		Properties prop = new Properties();
		File propFile = new File(propFilePath);
		try {
			if (!propFile.exists())
				propFile.createNewFile();
			prop.load(new FileReader(propFile));

			setProps(prop);
		} catch (IOException e1) {
			e1.printStackTrace();
			return;
		}
		try {
			this.jlime = Client.build(8);
			setClient(new BundlerClient(new StoreConfig(StoreType.LEVELDB,
					"/home/acorbellini/TwitterDB", "TwitterLevelDB"), 100000,
					jlime.getCluster()));
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	// List<String> lines = Collections
	// .<String> synchronizedList(new LinkedList<String>());

	RingQueue queue = new RingQueue(4000 * 1024);

	int printCountDown = 1000000;

	protected Logger log = Logger.getLogger(Loader.class);

	protected void load() throws Exception {
		//		getClient().list();

		// System.in.read();
		new Thread("Process Lines") {

			public void run() {
				try {
					TIntArrayList valueBuffer = new TIntArrayList();
					Integer currentKey = null;
					while (true) {
						Object[] lines = queue.take();
						for (Object object : lines) {
							if (object == null) {
								if (!valueBuffer.isEmpty()) {
									agregar(resolveKey(currentKey),
											valueBuffer.toArray());
									valueBuffer.clear();
								}
								try {
									getClient().close();
									jlime.close();
								} catch (Exception e) {
									e.printStackTrace();
								}
								return;
							}
							Integer k = getFirstUser((String) object);
							Integer v = getSecondUser((String) object);
							if (currentKey == null)
								currentKey = k;
							else if (!currentKey.equals(k)) {
								agregar(resolveKey(currentKey),
										valueBuffer.toArray());
								valueBuffer.clear();
								currentKey = k;
							}

							valueBuffer.add(v);

						}
					}
				} catch (IOException e) {
					e.printStackTrace();
				}
			}

		}.start();

		File f = new File(path);
		BufferedReader reader = null;
		try {
			reader = new BufferedReader(new FileReader(f), 1024 * 1024);
		} catch (FileNotFoundException e1) {
			e1.printStackTrace();
		}
		String line;
		try {
			while ((line = reader.readLine()) != null) {
				if (printCountDown == 0) {
					log.info(line);
					printCountDown = 1000000;
				} else
					printCountDown--;
				if (getFirstUser(line) >= empezarPorClave) {
					// lines.add(line);
					queue.put(line);
					// haveLines.release();
				}
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
		queue.put(null);

	}

	protected Integer resolveKey(Integer f) {
		return prefix ? -f : f;
	}

	protected void agregar(int k, int[] list) throws IOException {
		Arrays.sort(list);
		boolean done = false;
		int count = 0;
		while (!done)
			try {
				getClient().set(k, list);
				// System.out.println("Clave " + k + ": se seteo lista de " +
				// list.length + " seguidores/seguidos.");
				done = true;
			} catch (Exception e) {
				try {
					count++;
					log.warn("Clave " + k + ", intento " + count
							+ ": Hubo una excepci�n  al intentar agregar ["
							+ e.getMessage() + "].");
					Thread.sleep(5000);
				} catch (InterruptedException e1) {
					e1.printStackTrace();
				}
			}
	}

	private void setProps(Properties prop) {
		path = prop.getProperty("path");
		empezarPorClave = new Integer(prop.getProperty("clave_inicio"));
		prefix = new Boolean(prop.getProperty("prefix"));
	}

	public static void main(String[] args) throws Exception {
		new Loader(args[0]).load();
	}

	public String getPath() {
		return path;
	}

	public PersistentIntIntArrayMap getClient() {
		return client;
	}

	public void setClient(PersistentIntIntArrayMap client) {
		this.client = client;
	}

	private Integer getSecondUser(String line) {
		return new Integer(splitUsers(line)[1]);
	}

	private String[] splitUsers(String line) {
		line = line.trim().replaceAll("\\s+", " ").trim();
		String[] split = line.split("\\s");
		return split;
	}

	private Integer getFirstUser(String line) {
		return new Integer(splitUsers(line)[0]);
	};
}
