package edu.jlime.graphly.http.resource;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;

@Path("api")
public class GraphlyResource {

	@GET
	@Produces(MediaType.TEXT_PLAIN)
	@Path("hello")
	public String hello() {
		return "hello";
	}

	@GET
	@Produces(MediaType.TEXT_PLAIN)
	@Path("ping/{id}")
	public String ping(@PathParam("id") String id) {
		System.out.println("Received ping from " + id);
		return "ok";
	}
}
