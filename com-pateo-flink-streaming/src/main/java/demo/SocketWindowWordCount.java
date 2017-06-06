package demo;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

public class SocketWindowWordCount {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

//        Unbounded: Infinite datasets that are appended to continuously
//        Bounded: Finite, unchanging datasets
        DataStream<Tuple2<String, Integer>> dataStream = env
                .socketTextStream("shjq-np-test-hadoop-slave", 9999)
                .flatMap(new Splitter())
                .keyBy(0)
                //.timeWindow(Time.seconds(5)) // 添加窗口，并且设置窗口宽度
                .sum(1);

        dataStream.print();

        env.execute("Window WordCount");
    }

    public static class Splitter implements FlatMapFunction<String, Tuple2<String, Integer>> {
  
		private static final long serialVersionUID = 1L;

		@Override
        public void flatMap(String sentence, Collector<Tuple2<String, Integer>> out) throws Exception {
            for (String word: sentence.split(" ")) {
                out.collect(new Tuple2<String, Integer>(word, 1));
            }
        }
    }

}