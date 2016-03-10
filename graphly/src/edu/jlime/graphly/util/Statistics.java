package edu.jlime.graphly.util;

import java.io.File;
import java.io.FileWriter;

import edu.jlime.graphly.client.Graph;
import edu.jlime.graphly.server.GraphlyServer;

public class Statistics {
	public static void main(String[] args) throws Exception {
		GraphlyServer server = GraphlyServerFactory.loopback(args[0]).build();
		server.start();

		Graph cli = server.getGraphlyClient().getGraph(args[1]);
		System.out.println("Vertices: " + cli.getVertexCount());

		{
			FileWriter writer = new FileWriter(
					new File(args[2] + "/konect.ids"));
			for (Long l : cli.vertices()) {
				writer.append(l + "\n");
			}

			writer.close();
		}

//		{
//			TLongIntHashMap map = new TLongIntHashMap();
//			for (Long l : cli.vertices()) {
//				map.put(l, cli.getEdgesCount(Dir.IN, l, null));
//			}
//
//			FileWriter writer = new FileWriter(
//					new File(args[2] + "/followers.count"));
//			TLongIntIterator it = map.iterator();
//			while (it.hasNext()) {
//				it.advance();
//				writer.append(it.key() + "," + it.value() + "\n");
//			}
//			writer.close();
//		}
//
//		{
//			TLongIntHashMap map = new TLongIntHashMap();
//			for (Long l : cli.vertices()) {
//				map.put(l, cli.getEdgesCount(Dir.OUT, l, null));
//			}
//
//			FileWriter writer = new FileWriter(
//					new File(args[2] + "/followees.count"));
//			TLongIntIterator it = map.iterator();
//			while (it.hasNext()) {
//				it.advance();
//				writer.append(it.key() + "," + it.value() + "\n");
//			}
//			writer.close();
//		}

		server.stop();
	}
}
