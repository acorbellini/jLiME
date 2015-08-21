package edu.jlime.core.rpc.creator;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.lang.reflect.Method;
import java.lang.reflect.Parameter;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.lang.reflect.TypeVariable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;

import edu.jlime.core.rpc.Cache;
import edu.jlime.core.rpc.RPCTemplate;
import edu.jlime.core.rpc.Sync;
import edu.jlime.core.rpc.Wrappers;

public class RPCCreator {
	private class MethodSignature {
		public MethodSignature(String code, MethodParams params) {
			this.code = code;
			this.params = params;
		}

		String code;
		MethodParams params;
	}

	private class MethodParams {
		public MethodParams(String string, List<String> argsNames) {
			this.code = string;
			this.arguments = argsNames;
		}

		String code;
		List<String> arguments;
	}

	public static void main(String[] args) throws Exception {
		new RPCCreator().exec(args);

	}

	private void exec(String[] args) throws Exception {
		for (String iface : args) {

			Class<?> serverInterface = Class.forName(iface);

			// createBroadcastIface(serverInterface);

			createServer(serverInterface);

			createBroadcastServer(serverInterface);

			createFactory(serverInterface);
		}
	}

	/**
	 * @param serverInterface
	 * @throws IOException
	 */
	private void createFactory(Class<?> serverInterface) throws IOException {

		String iface = serverInterface.getSimpleName();

		String name = serverInterface.getSimpleName() + "Factory";

		String bcast = serverInterface.getSimpleName() + "Broadcast";

		String server = serverInterface.getSimpleName() + "Server";

		RPCTemplate factory = readTemplate(RPCCreator.class
				.getResourceAsStream("./factory.rpc"));

		factory.putReplacement("package", getPackage(serverInterface));
		factory.putReplacement("iface", iface);
		factory.putReplacement("name", name);
		factory.putReplacement("bcast", bcast);
		factory.putReplacement("server", server);

		BufferedWriter writer = new BufferedWriter(new FileWriter(new File(name
				+ ".java")));

		writer.append(factory.build());
		writer.close();
	}

	private RPCTemplate readTemplate(InputStream inputStream)
			throws IOException {
		StringBuilder builder = new StringBuilder();
		BufferedReader reader = new BufferedReader(new InputStreamReader(
				inputStream));
		while (reader.ready()) {
			builder.append(reader.readLine() + "\n");
		}
		reader.close();
		return new RPCTemplate(builder.toString());
	}

	private void createBroadcastServer(Class<?> serverInterface)
			throws Exception {

		createBroadcastIface(serverInterface);

		String name = serverInterface.getSimpleName() + "BroadcastImpl";

		BufferedWriter writer = new BufferedWriter(new FileWriter(new File(name
				+ ".java")));

		writer.write(getPackage(serverInterface));
		writer.write("import edu.jlime.core.cluster.BroadcastException;\n");
		writer.write(getImports(serverInterface));

		writer.write("public class " + name + " implements "
				+ serverInterface.getSimpleName() + "Broadcast {\n\n");

		writer.write(getBroadcastFields());

		writer.write(getBroadcastConstructor(name));

		for (Method method : serverInterface.getMethods()) {
			MethodSignature broadcastMethodSignature = getBroadcastMethodSignature(
					method.getName(), method);
			writer.write(broadcastMethodSignature.code);
			writer.write(getBroadcastBody(method,
					broadcastMethodSignature.params.arguments));
		}
		writer.write("}");
		writer.close();
	}

	private String getBroadcastBody(Method method, List<String> arguments) {
		StringBuilder builder = new StringBuilder();
		builder.append(" {\n");
		StringBuilder args = new StringBuilder();
		boolean firstArg = true;
		for (String arg : arguments) {
			if (firstArg)
				firstArg = false;
			else
				args.append(",");
			args.append(arg);
		}
		boolean sync = false;
		if (!method.getReturnType().getSimpleName().equals("void"))
			sync = true;
		else if (method.getAnnotation(Sync.class) != null)
			sync = true;

		String singleCall = sync ? "multiCall" : "multiCallAsync";

		if (!method.getReturnType().getSimpleName().equals("void"))
			builder.append("    return ");
		// Map<Peer," + retType + ">
		else
			builder.append("    ");
		builder.append("disp." + singleCall + "( dest, client, targetID, \""
				+ method.getName() + "\",new Object[] { " + args + " });\n");
		builder.append("  }\n\n");
		return builder.toString();
	}

	private void createServer(Class<?> serverInterface) throws Exception {
		String name = serverInterface.getSimpleName() + "ServerImpl";

		BufferedWriter writer = new BufferedWriter(new FileWriter(new File(name
				+ ".java")));

		writer.write(getPackage(serverInterface));

		writer.write(getImports(serverInterface));

		writer.write("public class " + name + " extends RPCClient implements "
				+ serverInterface.getSimpleName() + ", Transferible {\n\n");

		writer.write(getFields(serverInterface.getSimpleName(),
				serverInterface.getMethods()));

		writer.write(getConstructor(name, serverInterface.getSimpleName()));

		for (Method method : serverInterface.getMethods()) {
			MethodSignature methodSignature = getMethodSignature(
					method.getName(), method);
			writer.write(methodSignature.code);
			writer.write(getBody(serverInterface.getSimpleName(),
					method.getName(), method, methodSignature.params.arguments));
		}

		writer.write("@Override\n"
				+ "public void setRPC(RPCDispatcher rpc) {\n"
				+ "this.disp=rpc;\n"
				+ "this.localRPC = RPCDispatcher.getLocalDispatcher(super.dest);\n"
				+ "}\n");

		writer.write("public " + serverInterface.getSimpleName()
				+ " getLocal() throws Exception {" + "	if(local==null){"
				+ "		synchronized(this){" + "			if(local==null){"
				+ "				this.local = (" + serverInterface.getSimpleName()
				+ "							  ) localRPC.getTarget(targetID);\n" + "			}" + "		}"
				+ "}" + "" + "" + "\n" + "return this.local;\n" + "}\n");

		writer.write("}");
		writer.close();
	}

	private String getFields(String iface, Method[] methods) {
		StringBuilder builder = new StringBuilder();
		for (Method method : methods)
			if (method.getAnnotation(Cache.class) != null
					&& !method.getReturnType().getSimpleName().equals("void"))
				builder.append("   " + method.getReturnType().getSimpleName()
						+ " " + method.getName() + "Cached = null;\n");
		builder.append("   transient RPCDispatcher localRPC;\n");

		builder.append("   transient volatile " + iface + " local = null;\n");

		return builder.toString();
	}

	private String getPackage(Class<?> ifaceClass) {
		return ifaceClass.getPackage() + ";\n\n";
	}

	private String getBroadcastFields() {
		StringBuilder field = new StringBuilder();
		field.append("  RPCDispatcher disp;\n");
		field.append("  Peer local;\n");
		field.append("  List<Peer> dest = new ArrayList<Peer>();\n");
		field.append("  Peer client;\n");
		field.append("  String targetID;\n\n");
		return field.toString();
	}

	private String getConstructor(String name, String iface) {
		StringBuilder constructor = new StringBuilder();

		constructor
				.append("  public "
						+ name
						+ "(RPCDispatcher disp, Peer dest, Peer client, String targetID) {\n");
		constructor.append(" super(disp, dest, client, targetID);\n");

		// constructor
		constructor
				.append(" this.localRPC = RPCDispatcher.getLocalDispatcher(dest);");
		// constructor.append(" if(localRPC!=null)");
		// constructor.append(" 	this.local = (" + iface
		// + ") localRPC.getTarget(targetID);\n");
		constructor.append("}\n\n");

		return constructor.toString();
	}

	private String getBroadcastConstructor(String name) {
		StringBuilder constructor = new StringBuilder();
		constructor
				.append("  public "
						+ name
						+ "(RPCDispatcher disp, List<Peer> dest, Peer client, String targetID) {\n");
		constructor.append("    this.disp = disp;\n");
		constructor.append("    this.dest.addAll(dest);\n");
		constructor.append("    this.client = client;\n");
		constructor.append("    this.targetID = targetID;\n");
		constructor.append("  }\n\n");
		return constructor.toString();
	}

	private MethodSignature getMethodSignature(String name, Method method)
			throws Exception {
		String types = getTypes(method.getGenericParameterTypes());
		StringBuilder ret = new StringBuilder();
		String retType = method.getReturnType().getSimpleName();
		if (method.getGenericReturnType() instanceof TypeVariable)
			retType = method.getGenericReturnType().getTypeName();
		ret.append("   public " + (types.isEmpty() ? "" : "<" + types + "> ")
				+ retType + " " + name + "(");
		MethodParams parameters = getParameters(method);
		ret.append(parameters.code);
		ret.append(") ");
		ret.append(getExceptions(method));
		return new MethodSignature(ret.toString(), parameters);
	}

	private String getTypes(Type[] tp) {
		StringBuilder types = new StringBuilder();
		boolean first = true;
		for (Type typeVariable : tp) {
			if (typeVariable instanceof ParameterizedType) {
				Type[] actualTypeArguments = ((ParameterizedType) typeVariable)
						.getActualTypeArguments();
				types.append(getTypes(actualTypeArguments));
			} else if (typeVariable instanceof TypeVariable) {
				if (first)
					first = false;
				else
					types.append(",");
				types.append(typeVariable);
			}
		}
		return types.toString();
	}

	private MethodSignature getBroadcastMethodSignature(String name,
			Method method) throws Exception {
		String types = getTypes(method.getGenericParameterTypes());
		StringBuilder ret = new StringBuilder();
		String returnType = "void";
		if (!method.getReturnType().getSimpleName().equals("void")) {
			String simpleName = method.getReturnType().getSimpleName();
			Class wrapper = Wrappers.get(method.getReturnType());
			if (wrapper != null)
				simpleName = wrapper.getSimpleName();
			if (method.getGenericReturnType() instanceof TypeVariable)
				simpleName = method.getGenericReturnType().getTypeName();

			returnType = "Map<Peer," + simpleName + "> ";
		}
		ret.append("   public " + (types.isEmpty() ? "" : "<" + types + "> ")
				+ returnType + " " + name + "(");
		MethodParams parameters = getParameters(method);
		ret.append(parameters.code);
		ret.append(") throws Exception");
		return new MethodSignature(ret.toString(), parameters);
	}

	private String getExceptions(Method method) {
		StringBuilder ret = new StringBuilder();
		Class<?>[] types = method.getExceptionTypes();
		if (types.length != 0) {
			ret.append(" throws ");
			StringBuilder builder = new StringBuilder();
			for (Class<?> class1 : types) {
				builder.append("," + class1.getSimpleName());
			}
			ret.append(builder.substring(1));
		}
		return ret.toString();
	}

	private MethodParams getParameters(Method method) throws Exception {
		HashMap<String, HashSet<String>> used = new HashMap<>();
		List<String> argsNames = new ArrayList<>();
		StringBuilder ret = new StringBuilder();
		Parameter[] args = method.getParameters();
		if (args.length != 0) {
			StringBuilder argsBuilder = new StringBuilder();
			for (Parameter c : args) {
				int count = 0;
				String type = c.getType().getSimpleName();

				// if (c.getType().isPrimitive())
				// throw new Exception("Primitive arguments not supported");

				if (c.getType().getTypeParameters().length > 0) {

					Type pType = c.getParameterizedType();
					if (pType instanceof ParameterizedType) {
						boolean first = true;
						type = type + "<";
						ParameterizedType t = (ParameterizedType) c
								.getParameterizedType();
						for (Type parameter : t.getActualTypeArguments()) {
							if (first)
								first = false;
							else
								type += ",";
							type = type + parameter.getTypeName();
						}
						type = type + ">";
					} else {

						TypeVariable<?>[] parameterizedType = c.getType()
								.getTypeParameters();

						type = type + "<";
						boolean first = true;
						for (TypeVariable<?> parameter : parameterizedType) {
							if (first)
								first = false;
							else
								type += ",";
							try {
								getClass().getClassLoader().loadClass(
										parameter.getTypeName());
								type = type + parameter.getTypeName();
							} catch (Exception e) {
								type = type + "?";
							}

						}
						type = type + ">";
					}

				}
				String arg = c.getName();
				HashSet<String> set = used.get(type);
				if (set == null) {
					set = new HashSet<>();
					used.put(type, set);
				}
				while (arg == null || set.contains(arg) || type.equals(arg)) {
					if (arg == null)
						arg = c.getType().getSimpleName().toLowerCase();
					else {
						arg = arg + count;
						count++;
					}
				}
				set.add(arg);
				argsNames.add(arg);
				argsBuilder.append(", final " + type + " " + arg);
			}
			ret.append(argsBuilder.substring(2));
		}
		return new MethodParams(ret.toString(), argsNames);
	}

	private String getImports(Class<?> ifaceClass) {
		StringBuilder imports = new StringBuilder();
		imports.append("import " + ifaceClass.getName() + ";\n");
		imports.append("import edu.jlime.core.rpc.RPCDispatcher;\n");
		imports.append("import edu.jlime.core.rpc.RPCClient;\n");
		imports.append("import edu.jlime.core.cluster.Peer;\n");
		imports.append("import java.util.List;\n");
		imports.append("import java.util.ArrayList;\n");
		imports.append("import java.util.Map;\n");
		imports.append("import edu.jlime.core.rpc.Transferible;\n");

		for (Method m : ifaceClass.getMethods()) {
			for (Class<?> arg : m.getParameterTypes())
				if (!isPrimitive(arg))
					imports.append("import " + getName(arg) + ";\n");

			Class<?> ret = m.getReturnType();
			if (!isPrimitive(ret))
				imports.append("import " + getName(ret) + ";\n");

			for (Class<?> ex : m.getExceptionTypes()) {
				imports.append("import " + ex.getCanonicalName() + ";\n");
			}
		}
		return imports.toString() + "\n";
	}

	private String getName(Class<?> arg) {
		Class<?> curr = arg;
		while (curr.isArray())
			curr = curr.getComponentType();
		return curr.getName();
	}

	private boolean isPrimitive(Class<?> arg) {
		Class<?> curr = arg;
		while (!curr.isPrimitive()) {
			if (curr.isArray())
				curr = curr.getComponentType();
			else
				return false;
		}
		return true;
	}

	private String getBody(String iface, String rpcmethod, Method method,
			List<String> arguments) {
		StringBuilder builder = new StringBuilder();
		builder.append(" {\n");

		StringBuilder args = new StringBuilder();
		boolean firstArg = true;
		for (String arg : arguments) {
			if (firstArg)
				firstArg = false;
			else
				args.append(",");
			args.append(arg);
		}
		boolean sync = false;
		if (!method.getReturnType().getSimpleName().equals("void"))
			sync = true;
		else if (method.getAnnotation(Sync.class) != null)
			sync = true;

		boolean cached = false;
		if (method.getAnnotation(Cache.class) != null)
			cached = true;

		String retType = method.getReturnType().getSimpleName();
		if (method.getGenericReturnType() instanceof TypeVariable)
			retType = method.getGenericReturnType().getTypeName();

		String singleCall = sync ? "callSync" : "callAsync";

		String callCode = "disp." + singleCall + "(dest, client, targetID, \""
				+ method.getName() + "\",new Object[] { " + args + " });\n";

		builder.append("if(localRPC!=null) {\n");

		String simpleCall = "getLocal()." + rpcmethod + "(" + args + ")";

		if (!sync)
			simpleCall = "async.execute(new Runnable(){\n"
					+ "public void run(){\n" + "try{\n" + "          "
					+ simpleCall + ";\n"
					+ "} catch (Exception e) {e.printStackTrace();}" + "}\n"
					+ "});\n";

		if (!retType.equals("void")) {
			builder.append("		return ");
			builder.append(simpleCall + ";\n");
		} else {
			builder.append(simpleCall + ";\n");
			builder.append("		return;");
		}

		builder.append("}\n");
		if (cached && !retType.equals("void")) {
			builder.append("    if (" + method.getName() + "Cached==null){\n");
			builder.append("    	synchronized(this){\n");
			builder.append("    		if (" + method.getName() + "Cached==null)\n");
			builder.append("    			" + method.getName() + "Cached=(" + retType
					+ ") " + callCode + "\n");
			builder.append("    	}\n");
			builder.append("    }\n");
			builder.append("	return " + method.getName() + "Cached;\n");
		} else if (!retType.equals("void"))
			builder.append("    return (" + retType + ") " + callCode);
		else
			builder.append("    " + callCode);

		builder.append("  }\n\n");
		return builder.toString();
	}

	private void createBroadcastIface(Class<?> ifaceClass) throws Exception {
		String name = ifaceClass.getSimpleName() + "Broadcast";
		BufferedWriter writer = new BufferedWriter(new FileWriter(new File(name
				+ ".java")));
		writer.write(getPackage(ifaceClass));
		writer.write("import edu.jlime.core.cluster.BroadcastException;");
		writer.write(getImports(ifaceClass));
		writer.write("public interface " + name + " { \n\n");

		for (Method m : ifaceClass.getMethods())
			writer.write(getBroadcastMethodSignature(m.getName(), m).code
					+ "; \n\n");

		writer.write("}");
		writer.close();
	}
}
