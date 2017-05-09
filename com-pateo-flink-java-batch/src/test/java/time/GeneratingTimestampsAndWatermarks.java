package time;

import org.apache.flink.api.common.io.FileInputFormat;
import org.apache.flink.api.common.io.FilePathFilter;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.functions.AssignerWithPunctuatedWatermarks;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.functions.source.FileProcessingMode;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.api.windowing.time.Time;

import time.ProcessTimeExamples.MyEvent;

//
//There are two ways to assign timestamps and generate watermarks:
//Directly in the data stream source
//Via a timestamp assigner / watermark generator: in Flink timestamp assigners also define the watermarks to be emitted
//Both timestamps and watermarks are specified as millliseconds since the Java epoch of 1970-01-01T00:00:00Z
public class GeneratingTimestampsAndWatermarks {

	public static void main(String[] args) {
		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

		FileInputFormat myFormat = null;
		String myFilePath = null;
		TypeInformation typeInfo = null;
		DataStream<MyEvent> stream = env.readFile(myFormat, myFilePath, FileProcessingMode.PROCESS_CONTINUOUSLY, 100,
				typeInfo);

		// myFormat, myFilePath, FileProcessingMode.PROCESS_CONTINUOUSLY, 100,
		// FilePathFilter.createDefaultFilter(), typeInfo);

		DataStream<MyEvent> withTimestampsAndWatermarks = stream.filter(event -> event.severity() == "WARNING")
				.assignTimestampsAndWatermarks(new AssignerWithPeriodicWatermarks<ProcessTimeExamples.MyEvent>() {

					@Override
					public long extractTimestamp(MyEvent element, long previousElementTimestamp) {
						// TODO Auto-generated method stub
						return 0;
					}

					@Override
					public Watermark getCurrentWatermark() {
						// TODO Auto-generated method stub
						return null;
					}
				}) ;

		withTimestampsAndWatermarks.keyBy((event) -> event.getGroup()).timeWindow(Time.seconds(10))
				.reduce((a, b) -> a.add(b)).addSink(new SinkFunction<ProcessTimeExamples.MyEvent>() {

					private static final long serialVersionUID = 1L;

					@Override
					public void invoke(MyEvent value) throws Exception {

					}
				});

	}

	class MyType {

		public long getEventTimestamp() {
			return 0;
		}

		public boolean hasWatermarkTime() {
			return false;
		}

		public long getWatermarkTime() {

			return 0;
		}

	}

	private class MySourceFunction implements SourceFunction<MyType> {
		private static final long serialVersionUID = 1L;

		@Override
		public void run(org.apache.flink.streaming.api.functions.source.SourceFunction.SourceContext<MyType> ctx)
				throws Exception {
			while (true) {
				MyType next = getNext();
				ctx.collectWithTimestamp(next, next.getEventTimestamp());

				if (next.hasWatermarkTime()) {
					ctx.emitWatermark(new Watermark(next.getWatermarkTime()));
				}
			}
		}

		private MyType getNext() {
			return null;
		}

		@Override
		public void cancel() {

		}

	}

	class MyTimestampsAndWatermarks implements AssignerWithPeriodicWatermarks<MyEvent> {

		private static final long serialVersionUID = 1L;

		@Override
		public long extractTimestamp(MyEvent element, long previousElementTimestamp) {

			return 0;
		}

		@Override
		public Watermark getCurrentWatermark() {

			return null;
		}

	}
	
	public class BoundedOutOfOrdernessGenerator implements AssignerWithPeriodicWatermarks<MyEvent> {
 
		private static final long serialVersionUID = 1L;

		private final long maxOutOfOrderness = 3500; // 3.5 seconds

	    private long currentMaxTimestamp;

 	    public long extractTimestamp(MyEvent element, long previousElementTimestamp) {
	        long timestamp = element.getCreationTime();
	        currentMaxTimestamp = Math.max(timestamp, currentMaxTimestamp);
	        return timestamp;
	    }

	    @Override
	    public Watermark getCurrentWatermark() {
	        // return the watermark as current highest timestamp minus the out-of-orderness bound
	        return new Watermark(currentMaxTimestamp - maxOutOfOrderness);
	    }
	}

	/**
	 * This generator generates watermarks that are lagging behind processing time by a fixed amount.
	 * It assumes that elements arrive in Flink after a bounded delay.
	 */
	public class TimeLagWatermarkGenerator implements AssignerWithPeriodicWatermarks<MyEvent> {

		private static final long serialVersionUID = -5570547495067095462L;
		private final long maxTimeLag = 5000; // 5 seconds

		@Override
		public long extractTimestamp(MyEvent element, long previousElementTimestamp) {
			return element.getCreationTime();
		}

		@Override
		public Watermark getCurrentWatermark() {
			// return the watermark as current time minus the maximum time lag
			return new Watermark(System.currentTimeMillis() - maxTimeLag);
		}
	}
	
	
	public class PunctuatedAssigner implements AssignerWithPunctuatedWatermarks<MyEvent> {

		private static final long serialVersionUID = 1L;

		@Override
		public long extractTimestamp(MyEvent element, long previousElementTimestamp) {
			return element.getCreationTime();
		}

		@Override
		public Watermark checkAndGetNextWatermark(MyEvent lastElement, long extractedTimestamp) {
			return lastElement.hasWatermarkMarker() ? new Watermark(extractedTimestamp) : null;
		}
	}
}
