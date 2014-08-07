package edu.jlime.webmonitor;

import java.util.Map;

import javax.faces.bean.ManagedBean;
import javax.faces.bean.RequestScoped;
import javax.faces.context.FacesContext;

@ManagedBean(name = "pageBean")
@RequestScoped
public class PageBean {

	String page = "";

	public void setPage() {
		FacesContext context = FacesContext.getCurrentInstance();
		Map<String, String> map = context.getExternalContext()
				.getRequestParameterMap();
		this.page = (String) map.get("page");
	}

	public String getPage() {
		return page;
	}
}
