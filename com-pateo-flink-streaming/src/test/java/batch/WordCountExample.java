package batch;

import java.util.List;

import jdbc.JDBCInputFormat;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.operators.FlatMapOperator;
import org.apache.flink.api.java.operators.ProjectOperator;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.PojoField;
import org.apache.flink.api.java.typeutils.PojoTypeInfo;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;
import org.apache.flink.util.Collector;

public class WordCountExample {
    private static final TypeInformation STRING_TYPE_INFO = null;
	private static final TypeInformation INT_TYPE_INFO = null;

	public static void main(String[] args) throws Exception {
        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
    	final TypeInformation INT_TYPE_INFO = new PojoTypeInfo(String.class, new List<PojoField>());

        DataSet<String> localLines = env.readTextFile("./dep.txt");
        localLines.print();
        
        DataSet<String> text = env.fromElements(
            "Who's there?",
            "I think I hear them. Stand, ho! Who's there?");
        
     // Read data from a relational database using the JDBC input format
//        DataSet<Tuple2<String, Integer>  =  
        
        new TupleTypeInfo<Tuple2<String, String>>();
        
        env.createInput(JDBCInputFormat.buildJDBCInputFormat(), new TupleTypeInfo(Tuple2.class, Integer, INT_TYPE_INFO)) ;
        
        		DataSource dbData = env.createInput(
              // create and configure input format
              JDBCInputFormat.buildJDBCInputFormat()
                             .setDrivername("org.apache.derby.jdbc.EmbeddedDriver")
                             .setDBUrl("jdbc:derby:memory:persons")
                             .setQuery("select name, age from persons")
                             .finish(),
              // specify type information for DataSet
              new TupleTypeInfo(Tuple2.class, STRING_TYPE_INFO, INT_TYPE_INFO)
            );

        
        FlatMapOperator<String, Tuple2<String, Integer>> flatMap = text.flatMap(new LineSplitter());
        
        flatMap.max(0).print();
        
        //Selects a subset of fields from the tuples
        
        ProjectOperator<?, Tuple> project2 = flatMap.project(0);
        
        project2.print();
//        DataSet<Tuple2<String, Integer>> wordCounts = flatMap
//            .groupBy(0)
//            .sum(1).andMax(1);
//
//        wordCounts.print();
    }

    public static class LineSplitter implements FlatMapFunction<String, Tuple2<String, Integer>> {
      
		private static final long serialVersionUID = 1L;

		@Override
        public void flatMap(String line, Collector<Tuple2<String, Integer>> out) {
            for (String word : line.split(" ")) {
                out.collect(new Tuple2<String, Integer>(word, 1));
            }
        }
    }
}