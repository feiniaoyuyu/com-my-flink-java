package table;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.java.BatchTableEnvironment;
import org.apache.flink.table.sinks.TableSink;
import org.apache.flink.table.sources.CsvTableSource;
import org.apache.flink.table.sources.TableSource;

public class TableApiTest {
	

//	public static void main(String[] args) throws Exception {
//		ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
//		BatchTableEnvironment tEnv = TableEnvironment.getTableEnvironment(env) ;
//		
//		//env.readTextFile("test.csv").print();
//		
//		DataSet<Record> csvInput = env
//				.readCsvFile("test.csv")
//				.pojoType(Record.class, "playerName", "country", "year", "game", "gold", "silver", "bronze", "total");
// 		TableEnvironment tEnv;
//		// register the DataSet athletes as table "athletes" with fields derived  from the dataset
//		Table atheltes = tEnv.fromDataSet(csvInput);
//		tEnv.registerTable("athletesTable", atheltes);
//		// run a SQL query on the Table and retrieve the result as a new Table
//		Table groupedByCountry = tEnv.sql("SELECT country, SUM(total) as frequency FROM athletesTable group by country");
//
//		DataSet<Result> result = tEnv.toDataSet(groupedByCountry, Result.class);
//		result.print();
//
//		Table groupedByGame = atheltes
//				.select("game,total")
//				.groupBy("game")
//				.select("game, sum(total) as frequency");
//
//		DataSet<GameResult> gameResult = tEnv.toDataSet(groupedByGame, GameResult.class);
//
//		gameResult.print();
//		
//		DataSet<WC> input = env.fromElements(new WC("Hello", 1), new WC("World", 1), new WC("Hello", 1));
//		// register the DataSet as table "WordCount"
//		tEnv.registerDataSet("WordCount", input, "word, frequency");
//		//tEnv.sql("select * from WordCount limit 2").filter(predicate);
//		
//		Table selectedTable = tEnv.sql("SELECT word, SUM(frequency) as frequency FROM WordCount GROUP BY word having word = 'Hello'")
//				.where("word='hello'");
//				//.filter("word='hello'").as("word,frequency").limit(10);
//
//		selectedTable.select("word, frequency"); //select("*");
//		tEnv.registerTable("selected", selectedTable);
//		//Table scan = tEnv.scan("selected");
//		//tEnv.ingest("tableName"); // in StreamTableEnvironment
// 		
//	}
	
 	public static class WC {
		public String word;
		public long frequency;

		public WC() {
		}

		public WC(String word, long frequency) {
			this.word = word;
			this.frequency = frequency;
		}

		@Override
		public String toString() {
			return "WC " + word + " " + frequency;
		}
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
		public Integer frequency;
		public String game;

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
			return "GameResult [game=" + game + ", frequency=" + frequency + "]";
		}

	}
}
