package edu.jlime.core.marshalling;

import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.io.ObjectStreamClass;

import org.apache.log4j.Logger;

public class MarshallerInputStream extends ObjectInputStream {

	private String classSource;

	private ClassLoaderProvider disp;

	Logger log = Logger.getLogger(MarshallerInputStream.class);

	public MarshallerInputStream(InputStream is, ClassLoaderProvider disp,
			String classSourceID) throws IOException, SecurityException {
		super(is);
		this.disp = disp;
		this.classSource = classSourceID;
	}

	@Override
	protected Class<?> resolveClass(ObjectStreamClass desc)
			throws ClassNotFoundException {
		Exception e;
		String name = desc.getName();

		try {
			return super.resolveClass(desc);
		} catch (Exception ex) {
			e = ex;
		}
		try {
			return Class.forName(name);
		} catch (Exception ex) {
			e = ex;
		}

		try {
			return disp.loadClass(classSource, name);
		} catch (Exception e2) {
			e = e2;
		}
		throw new ClassNotFoundException(name, e);
	}
}
