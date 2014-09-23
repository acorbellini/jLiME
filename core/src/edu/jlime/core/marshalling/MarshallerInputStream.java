package edu.jlime.core.marshalling;

import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.io.ObjectStreamClass;

import org.apache.log4j.Logger;

import edu.jlime.core.cluster.Peer;
import edu.jlime.core.rpc.RPCDispatcher;
import edu.jlime.core.rpc.Transferible;

public class MarshallerInputStream extends ObjectInputStream {

	private Peer classSource;

	private RPCDispatcher disp;

	Logger log = Logger.getLogger(MarshallerInputStream.class);

	public MarshallerInputStream(InputStream is, RPCDispatcher rpcDispatcher,
			Peer classSourceID) throws IOException, SecurityException {
		super(is);
		this.disp = rpcDispatcher;
		this.classSource = classSourceID;
		enableResolveObject(true);
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

	@Override
	protected Object resolveObject(Object obj) throws IOException {
		if (Transferible.class.isAssignableFrom(obj.getClass()))
			((Transferible) obj).setRPC(disp);
		return obj;
	}

	public Object getRPC() {
		return disp;
	}
}
