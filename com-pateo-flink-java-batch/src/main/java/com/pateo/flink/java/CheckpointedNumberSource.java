package com.pateo.flink.java;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.time.ZoneId;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.zip.GZIPInputStream;

import org.apache.flink.streaming.api.checkpoint.ListCheckpointed;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction.SourceContext;
import org.apache.flink.streaming.api.watermark.Watermark;
 
 
class TaxiRide {
 
	public long rideId;
	public TaxiRide() {}

	public TaxiRide(long rideId ) {
		this.rideId = rideId;
 		//this.startTime = startTime;
	}
 
	@Override
	public boolean equals(Object other) {
		return other instanceof TaxiRide &&
				this.rideId == ((TaxiRide) other).rideId;
	}

	@Override
	public int hashCode() {
		return (int)this.rideId;
	}

	public static TaxiRide getTaxiRideWithId(Integer rideId) {
		return new TaxiRide(rideId);
	}
}

public class CheckpointedNumberSource implements SourceFunction<TaxiRide>, ListCheckpointed<Long> {
	 
		private static final long serialVersionUID = 1L;
 		private final int servingSpeed;
	 
		// state
		// number of emitted events
		private long eventCnt = 0;

		/**
		 * Serves the TaxiRide records from the specified and ordered gzipped input file.
		 * Rides are served out-of time stamp order with specified maximum random delay
		 * in a serving speed which is proportional to the specified serving speed factor.
		 *
		 * @param dataFilePath The gzipped input file from which the TaxiRide records are read.
		 */
		public CheckpointedNumberSource(String dataFilePath) {
			this(dataFilePath, 1);
		}

		/**
		 * Serves the TaxiRide records from the specified and ordered gzipped input file.
		 * Rides are served exactly in order of their time stamps
		 * in a serving speed which is proportional to the specified serving speed factor.
		 *
		 * @param dataFilePath The gzipped input file from which the TaxiRide records are read.
		 * @param servingSpeedFactor The serving speed factor by which the logical serving time is adjusted.
		 */
		public CheckpointedNumberSource(String dataFilePath, int servingSpeedFactor) {
 			this.servingSpeed = servingSpeedFactor;
		}

		@Override
		public void run(SourceContext<TaxiRide> sourceContext) throws Exception {

			final Object lock = sourceContext.getCheckpointLock();
			
			int cnt = 0;
			//int cntTotal = 10000;
			// skip emitted events
			while ( cnt <= eventCnt) {
				cnt++;
				TaxiRide ride = TaxiRide.getTaxiRideWithId(cnt );
 			}
   
		}
 
		@Override
		public void cancel() { }

		@Override
		public List<Long> snapshotState(long checkpointId, long checkpointTimestamp) throws Exception {
			return Collections.singletonList(eventCnt);
		}

		@Override
		public void restoreState(List<Long> state) throws Exception {
			for (Long s : state)
				this.eventCnt = s;
		}
}
