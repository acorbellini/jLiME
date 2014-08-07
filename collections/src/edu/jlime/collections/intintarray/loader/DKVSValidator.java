package edu.jlime.collections.intintarray.loader;

import java.util.logging.Logger;

public class DKVSValidator extends DKVSLoader {

	public DKVSValidator(String propFilePath) {
		super(propFilePath);
	}

	Logger log = Logger.getLogger(DKVSValidator.class.getName());

	@Override
	protected void agregar(int k, int[] list) {
		boolean done = false;
		while (!done)
			try {
				int[] get = getClient().get(k);

				if (get != null)
					if (get.length == list.length) {
						log.info("La clave: " + k + " es válida.");
					} else {
						log.info("El largo de los datos de la clave " + k
								+ " es " + get.length
								+ " y los datos originales tienen "
								+ list.length + ", intentando re-insertarla.");
						super.agregar(k, list);
					}
				else {
					log.info("La clave: " + k
							+ " es NULL, intentando re-insertarla.");
					super.agregar(k, list);
				}
				done = true;
			} catch (Exception e) {
				e.printStackTrace();
				try {
					Thread.sleep(5000);
				} catch (InterruptedException e1) {
					e1.printStackTrace();
				}
			}

	}

	public static void main(String[] args) throws Exception {
		new DKVSValidator(args[0]).load();
		System.exit(0);
	}
}
