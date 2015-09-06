package edu.jlime.util;

import java.util.List;

import edu.jlime.util.NetworkUtils.SelectedInterface;

public interface NetworkChangeListener {

	void interfacesChanged(List<SelectedInterface> added,
			List<SelectedInterface> removed);

}
