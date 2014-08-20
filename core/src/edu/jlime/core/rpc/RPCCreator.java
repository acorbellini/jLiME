package edu.jlime.core.rpc;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.lang.reflect.Method;
import java.lang.reflect.Parameter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;

public class RPCCreator {

	private static class MethodSignature {
		public MethodSignature(String code, MethodParams params) {
			this.code = code;
			this.params = params;
		}

		String code;
		MethodParams params;
	}

	private static class MethodParams {
		public MethodParams(String string, List<String> argsNames) {
			this.code = string;
			this.arguments = argsNames;
		}

		String code;
		List<String> arguments;
	}

	public static void main(String[] args) throws ClassNotFoundException,
			IOException {
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
	private static void createFactory(Class<?> serverInterface)
			throws IOException {

		String iface = serverInterface.getSimpleName();

		String name = serverInterface.getSimpleName() + "Factory";

		String bcast = serverInterface.getSimpleName() + "Broadcast";

		String server = serverInterface.getSimpleName() + "Server";

		BufferedWriter writer = new BufferedWriter(new FileWriter(new File(name
				+ ".java")));

		writer.write(getPackage(serverInterface));
		writer.write("import edu.jlime.core.cluster.Peer;\n");
		writer.write("import edu.jlime.core.rpc.RPCDispatcher;\n");
		writer.write("import edu.jlime.core.rpc.ClientFactory;\n");

		writer.write("import java.util.List;\n");

		writer.write("public class " + name + " implements ClientFactory<"
				+ iface + ">{\n\n");

		writer.write("  private RPCDispatcher rpc;\n\n");
		writer.write("  private String target;\n\n");

		writer.write("  public " + name
				+ "(RPCDispatcher rpc, String target){\n");
		writer.write("     this.rpc = rpc;\n");
		writer.write("     this.target = target;\n");
		writer.write("  }\n");

		writer.write("  public " + iface
				+ " getBroadcast(List<Peer> to, Peer client){\n");
		writer.write("    return new " + bcast + "(rpc, to, client, target);\n");
		writer.write("  }\n");

		writer.write("  public " + iface + " get(Peer to, Peer client){\n");
		writer.write("    return new " + server
				+ "Impl(rpc, to, client, target);\n");
		writer.write("  }\n");
		writer.write("}");
		writer.close();
	}

	private static void createBroadcastServer(Class<?> serverInterface)
			throws IOException {
		String name = serverInterface.getSimpleName() + "Broadcast";

		BufferedWriter writer = new BufferedWriter(new FileWriter(new File(name
				+ ".java")));

		writer.write(getPackage(serverInterface));
		writer.write("import edu.jlime.core.cluster.BroadcastException;");
		writer.write(getImports(serverInterface));

		writer.write("public class " + name + " implements "
				+ serverInterface.getSimpleName() + " {\n\n");

		writer.write(getBroadcastFields());

		writer.write(getBroadcastConstructor(name));

		for (Method method : serverInterface.getMethods()) {
			MethodSignature broadcastMethodSignature = getBroadcastMethodSignature(
					method.getName(), method);
			writer.write(broadcastMethodSignature.code);
			writer.write(getBroadcastBody(method.getName(), method,
					broadcastMethodSignature.params.arguments));
		}
		writer.write("}");
		writer.close();
	}

	private static String getBroadcastBody(String name, Method method,
			List<String> arguments) {
		StringBuilder builder = new StringBuilder();
		builder.append(" {\n");
		StringBuilder args = new StringBuilder();
		for (String arg : arguments) {
			args.append("," + arg);
		}
		boolean sync = !method.getReturnType().getSimpleName().equals("void");
		String retType = method.getReturnType().getSimpleName();
		String singleCall = sync ? "multiCallSync" : "multiCallAsync";

		if (sync)
			builder.append("    return Map<Peer," + retType + "> ");
		else
			builder.append("    ");
		builder.append("disp." + singleCall + "( dest, client, targetID, \""
				+ method.getName() + "\",new Object[] { " + args.substring(1)
				+ " });\n");
		builder.append("  }\n\n");
		return builder.toString();
	}

	private static void createServer(Class<?> serverInterface)
			throws IOException {
		String name = serverInterface.getSimpleName() + "ServerImpl";

		BufferedWriter writer = new BufferedWriter(new FileWriter(new File(name
				+ ".java")));

		writer.write(getPackage(serverInterface));

		writer.write(getImports(serverInterface));

		writer.write("public class " + name + " extends RPCClient implements "
				+ serverInterface.getSimpleName() + " {\n\n");

		// writer.write(getFields());

		writer.write(getConstructor(name));

		for (Method method : serverInterface.getMethods()) {
			MethodSignature methodSignature = getMethodSignature(
					method.getName(), method);
			writer.write(methodSignature.code);
			writer.write(getBody(method.getName(), method,
					methodSignature.params.arguments));
		}
		writer.write("}");
		writer.close();
	}

	private static String getPackage(Class<?> ifaceClass) {
		return ifaceClass.getPackage() + ";\n\n";
	}

	private static String getBroadcastFields() {
		StringBuilder field = new StringBuilder();
		field.append("  RPCDispatcher disp;\n");
		field.append("  Peer local;\n");
		field.append("  List<Peer> dest = new ArrayList<Peer>();\n");
		field.append("  Peer client;\n");
		field.append("  String targetID;\n\n");
		return field.toString();
	}

	private static String getConstructor(String name) {
		StringBuilder constructor = new StringBuilder();

		constructor
				.append("  public "
						+ name
						+ "(RPCDispatcher disp, Peer dest, Peer client, String targetID) {\n");
		constructor.append(" super(disp, dest, client, targetID);\n");
		constructor.append("  }\n\n");
		return constructor.toString();
	}

	private static String getBroadcastConstructor(String name) {
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

	private static MethodSignature getMethodSignature(String name, Method method)
			throws IOException {
		StringBuilder ret = new StringBuilder();
		ret.append("  public " + method.getReturnType().getSimpleName() + " "
				+ name + "(");
		MethodParams parameters = getParameters(method);
		ret.append(parameters.code);
		ret.append(") ");
		ret.append(getExceptions(method));
		return new MethodSignature(ret.toString(), parameters);
	}

	private static MethodSignature getBroadcastMethodSignature(String name,
			Method method) throws IOException {
		StringBuilder ret = new StringBuilder();
		String returnType = method.getReturnType().getSimpleName()
				.equals("void") ? "void" : "Map<Peer,"
				+ method.getReturnType().getSimpleName() + "> ";
		ret.append("  public " + returnType + " " + name + "(");
		MethodParams parameters = getParameters(method);
		ret.append(parameters.code);
		ret.append(") throws Exception");
		return new MethodSignature(ret.toString(), parameters);
	}

	private static String getExceptions(Method method) {
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

	private static MethodParams getParameters(Method method) {
		HashMap<String, HashSet<String>> used = new HashMap<>();
		List<String> argsNames = new ArrayList<>();
		StringBuilder ret = new StringBuilder();
		Parameter[] args = method.getParameters();
		if (args.length != 0) {
			StringBuilder argsBuilder = new StringBuilder();
			for (Parameter c : args) {
				int count = 0;
				String type = c.getType().getSimpleName();
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
				argsBuilder.append(", " + type + " " + arg);
			}
			ret.append(argsBuilder.substring(2));
		}
		return new MethodParams(ret.toString(), argsNames);
	}

	private static String getImports(Class<?> ifaceClass) {
		StringBuilder imports = new StringBuilder();
		imports.append("import " + ifaceClass.getName() + ";\n");
		imports.append("import edu.jlime.core.rpc.RPCDispatcher;\n");
		imports.append("import edu.jlime.core.rpc.RPCClient;\n");
		imports.append("import edu.jlime.core.cluster.Peer;\n");
		imports.append("import java.util.List;\n");
		imports.append("import java.util.ArrayList;\n");

		for (Method m : ifaceClass.getMethods()) {
			for (Class<?> arg : m.getParameterTypes()) {
				if (!isPrimitive(arg)) {
					imports.append("import " + getName(arg) + ";\n");
				}
				Class<?> ret = m.getReturnType();
				if (!isPrimitive(ret))
					imports.append("import " + getName(ret) + ";\n");
			}
			for (Class<?> ex : m.getExceptionTypes()) {
				imports.append("import " + ex.getCanonicalName() + ";\n");
			}
		}
		return imports.toString() + "\n";
	}

	private static String getName(Class<?> arg) {
		Class<?> curr = arg;
		while (curr.isArray())
			curr = curr.getComponentType();
		return curr.getName();
	}

	private static boolean isPrimitive(Class<?> arg) {
		Class<?> curr = arg;
		while (!curr.isPrimitive()) {
			if (curr.isArray())
				curr = curr.getComponentType();
			else
				return false;
		}
		return true;
	}

	private static String getBody(String rpcmethod, Method method,
			List<String> arguments) {
		StringBuilder builder = new StringBuilder();
		builder.append(" {\n");
		StringBuilder args = new StringBuilder();
		for (String arg : arguments) {
			args.append("," + arg);
		}
		boolean sync = !method.getReturnType().getSimpleName().equals("void");
		String retType = method.getReturnType().getSimpleName();
		String singleCall = sync ? "callSync" : "callAsync";
		if (sync)
			builder.append("    return (" + retType + ") ");
		else
			builder.append("    ");
		builder.append("disp." + singleCall + "(dest, client, targetID, \""
				+ method.getName() + "\",new Object[] { " + args.substring(1)
				+ " });\n");
		builder.append("  }\n\n");
		return builder.toString();
	}
}
