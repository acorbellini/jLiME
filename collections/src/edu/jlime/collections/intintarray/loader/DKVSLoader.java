package edu.jlime.collections.intintarray.loader;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.Semaphore;

import org.apache.log4j.Logger;

import edu.jlime.client.Client;
import edu.jlime.collections.intintarray.client.BundlerClient;
import edu.jlime.collections.intintarray.client.PersistentIntIntArrayMap;
import edu.jlime.collections.intintarray.client.jobs.StoreConfig;
import edu.jlime.collections.intintarray.db.StoreFactory.StoreType;
import gnu.trove.list.array.TIntArrayList;

public class DKVSLoader {

	private PersistentIntIntArrayMap client = null;

	private String path;

	private Integer empezarPorClave;

	private Boolean prefix;

	public DKVSLoader(String propFilePath) {
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
			setClient(new BundlerClient(new StoreConfig(StoreType.LEVELDB,
					"/home/acorbellini/TwitterDB", "TwitterLevelDB"), 100000,
					Client.build(8).getCluster()));
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	List<String> lines = Collections
			.<String> synchronizedList(new LinkedList<String>());

	Semaphore control = new Semaphore(5000000);

	Semaphore haveLines = new Semaphore(0);

	int printCountDown = 1000000;

	protected Logger log = Logger.getLogger(DKVSLoader.class);

	protected void load() throws Exception {
		new Thread("Read Lines") {
			public void run() {
				File f = new File(path);
				BufferedReader reader = null;
				try {
					reader = new BufferedReader(new FileReader(f));
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
							try {
								control.acquire();
							} catch (InterruptedException e) {
								e.printStackTrace();
							}
							lines.add(line);
							haveLines.release();
						}
					}
				} catch (IOException e) {
					e.printStackTrace();
				}
				lines.add(null);
				haveLines.release();
			};
		}.start();

		new Thread("Process Lines") {

			TIntArrayList followeesBuffer = new TIntArrayList();
			Integer follower = null;

			public void run() {
				boolean finished = false;
				while (!finished) {
					try {
						haveLines.acquire();
					} catch (InterruptedException e1) {
						e1.printStackTrace();
					}
					String currentLine = lines.remove(0);
					control.release();
					try {
						// Si el archivo termina, agrego lo que tengo en buffer
						// (si
						// tengo).
						if (currentLine == null) {
							try {
								agregar(resolveKey(follower),
										followeesBuffer.toArray());
							} catch (NumberFormatException e) {
								e.printStackTrace();
							} catch (IOException e) {
								e.printStackTrace();
							}
							finished = true;
							// Si no, agrego al buffer.
						} else {
							Integer followerActual = getFirstUser(currentLine);
							Integer followee = getSecondUser(currentLine);

							if (follower == null)
								follower = followerActual;
							if (!followerActual.equals(follower)) {
								try {
									agregar(resolveKey(follower),
											followeesBuffer.toArray());
								} catch (NumberFormatException e) {
									e.printStackTrace();
								} catch (IOException e) {
									e.printStackTrace();
								}
								follower = followerActual;
								followeesBuffer.clear();
							}
							followeesBuffer.add(new Integer(followee));

						}
					} catch (Exception e) {
						log.info("Wrongly formatted line " + currentLine);
					}
				}
			}

		}.start();
	}

	protected Integer resolveKey(Integer f) {
		return prefix ? -f : f;
	}

	protected void agregar(int k, int[] list) throws IOException {
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
		new DKVSLoader(args[0]).load();
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
