package edu.jlime.collections.intintarray.db;

public class StoreFactory {

	private String storeClass;

	public StoreFactory(String storeClass) {
		this.storeClass = storeClass;
	}

	public StoreFactory(StoreType type) {
		this.type = type;
	}

	public enum StoreType {

		H2,

		LEVELDB,

		OTHER
	}

	public StoreType type = StoreType.OTHER;

	public void setType(StoreType type) {
		this.type = type;
	}

	public Store getStore(String path, String name) {
		switch (type) {
		case H2:
			return new H2(name, path);
		case LEVELDB:
			// return new LevelDb(name, path);
		default:
			if (storeClass != null) {
				try {
					Class<?> sc = Class.forName(storeClass);
					Store store = ((Store) sc.getConstructor(String.class,
							String.class).newInstance(name, path));
					return store;
				} catch (Exception e) {
					e.printStackTrace();
				}
			}
			break;
		}
		return null;
	};
}
