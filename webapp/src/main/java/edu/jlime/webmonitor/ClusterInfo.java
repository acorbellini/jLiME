package edu.jlime.webmonitor;

import java.io.IOException;
import java.io.PrintWriter;

import javax.servlet.ServletException;
import javax.servlet.annotation.WebServlet;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import edu.jlime.jd.JobCluster;
import edu.jlime.jd.JobNode;

@WebServlet("/info")
public class ClusterInfo extends HttpServlet {

	private static final long serialVersionUID = 1L;

	public ClusterInfo() {
		super();
	}

	protected void doGet(HttpServletRequest request,
			HttpServletResponse response) throws ServletException, IOException {
		doPost(request, response);
	}

	protected void doPost(HttpServletRequest request,
			HttpServletResponse response) throws ServletException, IOException {
		response.setContentType("text/json");
		StringBuilder builder = new StringBuilder();
		JobCluster c = (JobCluster) getServletContext().getAttribute("cluster");
		for (JobNode p : c.getExecutors()) {
			builder.append(",{");
			builder.append("\"ip\":");
			builder.append("\"" + p.getName() + "\"");
			builder.append(",");
			builder.append("\"info\":");
			try {
				builder.append(p.getInfo() + "}");
			} catch (Exception e) {
				e.printStackTrace();
			}

		}
		PrintWriter writer = response.getWriter();
		writer.println("[" + builder.substring(1) + "]");
		writer.close();
	}

}
