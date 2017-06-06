package table;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.java.BatchTableEnvironment;
import org.apache.flink.util.Collector;

public class BatchJob {

	public static void main(String[] args) throws Exception {
		ExecutionEnvironment env = ExecutionEnvironment
				.getExecutionEnvironment();

		DataSet<Record> csvInput = env.readCsvFile("test.csv").pojoType(
				Record.class, "playerName", "country", "year", "game", "gold",
				"silver", "bronze", "total");

//		DataSet<Tuple2<String, Integer>> groupedByCountry = csvInput .flatMap(
//						new FlatMapFunction<Record, Tuple2<String, Integer>>() {
//							private static final long serialVersionUID = 1L;
//
//							@Override
//							public void flatMap(Record record,
//									Collector<Tuple2<String, Integer>> out)
//									throws Exception {
//								out.collect(new Tuple2<String, Integer>(record
//										.getCountry(), 1));
//							}
//						}).groupBy(0).sum(1);
//		
//		groupedByCountry.print();
//		
//		DataSet<Tuple2<String, Integer>> groupedByGame = csvInput
//				.flatMap(
//						new FlatMapFunction<Record, Tuple2<String, Integer>>() {
//							private static final long serialVersionUID = 1L;
//
//							@Override
//							public void flatMap(Record record,
//									Collector<Tuple2<String, Integer>> out)
//									throws Exception {
//								out.collect(new Tuple2<String, Integer>(record
//										.getGame(), 1));
//							}
//						}).groupBy(0).sum(1);
//		
//		groupedByGame.print();

		// Next we need to create a Table with this dataset and register it in
		// Table Environment for further
		// processing:
		BatchTableEnvironment tableEnv = TableEnvironment
				.getTableEnvironment(env);
		Table atheltes = tableEnv.fromDataSet(csvInput);
		tableEnv.registerTable("athletes", atheltes);
		// Next we can write a regular SQL query to get more insights from the
		// data. Or else we can use
		// Table API operators to manipulate the data, as shown in the following
		// code snippet:
//		Table groupedByCountryTable = tableEnv
//				.sql("SELECT country, SUM(total) as frequency FROM athletes group by country");
//		DataSet<Result> result = tableEnv.toDataSet(groupedByCountryTable,
//				Result.class);
//		result.print();
		Table groupedByGameTable = atheltes.groupBy("game").select("game, SUM(total) as frequency");
		DataSet<GameResult> gameResult = tableEnv
				.toDataSet(groupedByGameTable,GameResult.class);
		
		gameResult.print();
		
//		env.execute("--==");
	}

	public static class Result {
		public String country;
		public Integer frequency;

		public Result() {
			super();
		}

		public Result(String country, Integer total) {
			this.country = country;
			this.frequency = total;
		}

		@Override
		public String toString() {
			return "Result " + country + " " + frequency;
		}
	}

	public static class GameResult {
		public String game;
		public Integer frequency;

		public GameResult(String game, Integer frequency) {
			super();
			this.game = game;
			this.frequency = frequency;
		}

		public GameResult() {
			super();
		}

		@Override
		public String toString() {
			return "GameResult [game=" + game + ", frequency=" + frequency
					+ "]";
		}

	}
}
