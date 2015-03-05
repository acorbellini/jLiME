package edu.jlime.collections.intintarray.db;

import edu.jlime.util.DataTypeUtils;
import gnu.trove.list.array.TLongArrayList;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Arrays;

public class H2 extends Store {

	private Connection conn;

	private String storeName;

	private String storePath;

	public H2(String name, String storePath) {
		super(name);
		this.storePath = storePath;
		this.storeName = "DKVSDB";
		try {
			open();
			// conn.setTransactionIsolation(level);
			PreparedStatement stmt = conn
					.prepareStatement("CREATE TABLE IF NOT EXISTS " + storeName
							+ " (" + "key INTEGER PRIMARY KEY," + "data BLOB"
							+ ");");
			stmt.execute();
			stmt.close();
			conn.commit();
		} catch (SQLException e) {
			e.printStackTrace();
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		}

	}

	private void open() throws ClassNotFoundException, SQLException {
		if (conn != null && !conn.isClosed())
			return;
		Class.forName("org.h2.Driver");
		conn = DriverManager
				.getConnection(
						"jdbc:h2:file:" + storePath + "/"
								+ storeName
								+ ""
								// +
								// ";MVCC=TRUE;CACHE_SIZE=131072",
								+ ";LOG=0;LOCK_MODE=0;UNDO_LOG=0;ACCESS_MODE_DATA=r;CACHE_SIZE=1048576",
						"sa", "");
		conn.setAutoCommit(false);
	}

	@Override
	public byte[] load(long key) {
		try {
			if (conn == null || conn.isClosed())
				open();
		} catch (ClassNotFoundException e1) {
			e1.printStackTrace();
		} catch (SQLException e1) {
			e1.printStackTrace();
		}
		try {

			PreparedStatement stmt = conn
					.prepareStatement("SELECT key,data FROM " + storeName
							+ " WHERE key=" + key + ";");
			ResultSet rs = stmt.executeQuery();
			if (rs.next()) {
				// if (!rs.next()) {
				InputStream is = rs.getBinaryStream(2);
				ByteArrayOutputStream out = new ByteArrayOutputStream(
						512 * 1024);
				byte[] buffer = new byte[16384];
				int count = 0;
				while ((count = is.read(buffer)) != -1)
					out.write(buffer, 0, count); // copy streams

				// byte[] bs = rs.getBytes(2);
				// System.out.println("returning key=" + key + " data size:"
				// + bs.length);

				return out.toByteArray();
			}
			// System.out.println("No hay datos!!");
		} catch (SQLException e) {
			e.printStackTrace();
		} catch (IOException e) {

			e.printStackTrace();
		}
		return null;
	}

	@Override
	public void store(long k, byte[] bs) {
		try {
			// System.out.println("Saving key=" + k + " data size:" +
			// bs.length);
			PreparedStatement stmt = conn.prepareStatement("SELECT key FROM "
					+ storeName + " WHERE key=" + k + ";");
			ResultSet rs = stmt.executeQuery();
			PreparedStatement updateStmt = null;
			if (rs.first())
				updateStmt = conn.prepareStatement("UPDATE " + storeName
						+ " SET key=?, data=? WHERE key=" + k + ";");
			else
				updateStmt = conn.prepareStatement("INSERT INTO " + storeName
						+ " VALUES(?,?);");

			stmt.close();

			updateStmt.setLong(1, k);
			updateStmt.setBinaryStream(2, new ByteArrayInputStream(bs),
					bs.length);
			updateStmt.execute();
			updateStmt.close();
			rs.close();
			// conn.commit();

			// stmt=conn.prepareStatement("SELECT key FROM "
			// + storeName+";");
			// ResultSet res = stmt.executeQuery();
			// res.

		} catch (Exception e) {
			e.printStackTrace();
		}

	}

	@Override
	public void commit() {
		try {
			conn.commit();
		} catch (SQLException e) {
			e.printStackTrace();
		}
		;
	}

	@Override
	public void close() {
		try {
			conn.close();
		} catch (SQLException e) {
			e.printStackTrace();
		}

	}

	public static void main(String[] args) throws IOException {
		H2 h2 = new H2(null, "D:/TwitterAdjacencyGraph");

		// int[] byteArrayToIntArray = DataTypeUtils.byteArrayToIntArray(h2
		// .load(12));
		// Arrays.sort(byteArrayToIntArray);
		// System.out.println(Arrays.toString(byteArrayToIntArray));
		// int[] byteArrayToIntArray2 = DataTypeUtils.byteArrayToIntArray(h2
		// .load(-12));
		// Arrays.sort(byteArrayToIntArray2);
		// System.out.println(Arrays.toString(byteArrayToIntArray2));

		h2.dumpAll("D:/tweet-adj.txt");
		h2.close();

	}

	private void dumpAll(String file) throws IOException {
		File f = new File(file);
		if (!f.exists())
			f.createNewFile();

		TLongArrayList list = getUsers();
		list.sort();
		FileWriter writer = new FileWriter(f);
		for (int i = 0; i < list.size(); i++) {
			long user = list.get(i);

			int[] sub = DataTypeUtils.byteArrayToIntArray(load(user));

			Arrays.sort(sub);

			if (user < 0)
				user = -user;
			for (int l : sub) {
				writer.append(user + " " + l + "\n");
			}

		}
		writer.close();
	}

	private TLongArrayList getUsers() {
		TLongArrayList ret = new TLongArrayList();
		try {
			if (conn == null || conn.isClosed())
				open();
		} catch (ClassNotFoundException e1) {
			e1.printStackTrace();
		} catch (SQLException e1) {
			e1.printStackTrace();
		}
		try {

			PreparedStatement stmt = conn.prepareStatement("SELECT key FROM "
					+ storeName + " WHERE key>0;");
			ResultSet rs = stmt.executeQuery();
			while (rs.next()) {
				ret.add(rs.getLong(1));
			}
		} catch (SQLException e) {
			e.printStackTrace();
		}
		return ret;
	}
}
