package edu.jlime.util;

import java.io.BufferedReader;
import java.io.File;
import java.io.InputStreamReader;

public class CommandLineUtils {

	private static boolean CYGWIN = System.getProperty("os.name").startsWith("Windows") ? true : false;

	public static String execCommand(String cmd) throws Exception {
		ProcessBuilder procbuilder = null;
		if (CYGWIN) {
			File dir = new File("C:/cygwin/bin/bash.exe");
			File dir64 = new File("C:/cygwin64/bin/bash.exe");

			cmd = cmd.replaceAll("\"", "\\\\\"");
			cmd = "\"" + cmd + "\"";

			if (dir.exists())
				procbuilder = new ProcessBuilder("C:/cygwin/bin/bash.exe", "-c", cmd);
			else if (dir64.exists())
				procbuilder = new ProcessBuilder("C:/cygwin64/bin/bash.exe", "-c", cmd);
			else
				throw new Exception("Cygwin not found");

		} else
			procbuilder = new ProcessBuilder("/bin/sh", "-c", cmd);

		Process proc = procbuilder.start();
		BufferedReader br = new BufferedReader(new InputStreamReader(proc.getInputStream()));
		StringBuilder outputBuilder = new StringBuilder();
		String line = null;
		while ((line = br.readLine()) != null) {
			outputBuilder.append(line);
			outputBuilder.append(System.getProperty("line.separator"));
		}
		BufferedReader errorReader = new BufferedReader(new InputStreamReader(proc.getErrorStream()));
		StringBuilder errorBuilder = new StringBuilder();
		String errorLine = null;
		while ((errorLine = errorReader.readLine()) != null) {
			errorBuilder.append(errorLine);
			errorBuilder.append(System.getProperty("line.separator"));
		}

		String error = errorBuilder.toString();
		if (!error.isEmpty())
			throw new Exception("Error executing command '" + cmd + "' : \n " + error);

		return outputBuilder.toString();

	}
}
