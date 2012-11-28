package com.linkedin.databus2.producers.db;

import java.util.List;

import com.linkedin.databus.core.UnsupportedKeyException;
import com.linkedin.databus2.core.DatabusException;
import com.linkedin.databus2.producers.EventCreationException;

public interface SourceDBEventReader {

	ReadEventCycleSummary readEventsFromAllSources(
			long sinceSCN) throws DatabusException, EventCreationException,
			UnsupportedKeyException;

	List<MonitoredSourceInfo> getSources();
}
