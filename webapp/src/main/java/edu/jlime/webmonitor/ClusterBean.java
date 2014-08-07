package edu.jlime.webmonitor;

import java.io.Serializable;
import java.util.ArrayList;

import javax.faces.bean.ManagedBean;
import javax.faces.bean.SessionScoped;
import javax.faces.context.FacesContext;
import javax.servlet.ServletContext;

import edu.jlime.jd.JobCluster;
import edu.jlime.jd.JobNode;

@ManagedBean(name = "clusterBean")
@SessionScoped
public class ClusterBean implements Serializable {

	private static final long serialVersionUID = 1L;

	private JobCluster getClusterFromContext() {
		ServletContext ctx = (ServletContext) FacesContext.getCurrentInstance()
				.getExternalContext().getContext();
		return ((JobCluster) ctx.getAttribute("cluster"));
	}

	public ArrayList<PeerData> getCluster() {
		try {
			ArrayList<PeerData> list = new ArrayList<>();
			for (JobNode p : getClusterFromContext().getExecutors())

				list.add(new PeerData(p.getName(), p.getInfo().toString()));
			return list;
		} catch (Exception e) {
			e.printStackTrace();
		}
		return null;

	}

}
