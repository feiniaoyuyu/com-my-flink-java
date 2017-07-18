package graph;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.graph.Edge;
import org.apache.flink.graph.Graph;
import org.apache.flink.graph.GraphCsvReader;
import org.apache.flink.graph.Vertex;
import org.apache.flink.graph.asm.translate.TranslateFunction;
import org.apache.flink.types.LongValue;

public class GraphTest {

	public static void main(String[] args) throws Exception {
		Vertex<Long, String> v = new Vertex<Long, String>(1L, "foo");
		// A vertex with a Long ID and no value
		// Vertex<Long, NullValue> v = new Vertex<Long,
		// NullValue>(1L,NullValue.getInstance());

		// Edge connecting Vertices with Ids 1 and 2 having weight 0.5
		Edge<Long, Double> e = new Edge<Long, Double>(1L, 2L, 0.5);
		Double weight = e.getValue(); // weight = 0.5

		// A vertex with a Long ID and a String value
		Vertex<Long, String> v1 = new Vertex<Long, String>(1L, "foo");
		// A vertex with a Long ID and a String value
		Vertex<Long, String> v2 = new Vertex<Long, String>(2L, "bar");
		// Edge connecting Vertices with Ids 1 and 2 having weight 0.5
		Edge<Long, Double> e1 = new Edge<Long, Double>(1L, 2L, 0.5);
		Edge<Long, Double> e2 = new Edge<Long, Double>(2L, 1L, 0.5);

		
		String vertices = "path/to/vertex/input.csv"; 
		String edges = "path/to/edge/input.csv"; 
		ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

		DataSet<Tuple2<String, Long>> vertexTuples = env
				.readCsvFile(vertices)
				.types(String.class, Long.class);
		
		DataSet<Tuple3<String, String, Double>> edgeTuples = env
				.readCsvFile(edges)
				.types(String.class, String.class, Double.class);
		
		Graph<String, Long, Double> graph = Graph
				.fromTupleDataSet( vertexTuples, edgeTuples, env);
		
		GraphCsvReader fromCsvReader = Graph.fromCsvReader(vertices, edges, env);
		DataSet<Edge<String, Double>> edges2 = graph.getEdges();
		DataSet<Vertex<String, Long>> vertices2 = graph.getVertices();
		Graph<String, Long, Double> graph2 = Graph.fromDataSet(vertices2, edges2, env);
		
		// increment each vertex value by 5
		Graph<String, Long, Double> updatedGraph = graph2;
		updatedGraph.mapVertices(new MapFunction<Vertex<String,Long>, Long>() {
 
			private static final long serialVersionUID = 1L;

			@Override
			public Long map(Vertex<String, Long> value) throws Exception {
				// TODO Auto-generated method stub
					return value.getValue() + 5;
			}
		});
				
		//Graph<String, Long, Double> updatedGraph2 = 
		Graph<String, Long, Double> translateGraphIds = graph.translateGraphIds(new TranslateFunction<String, String>() {
 
			private static final long serialVersionUID = 1L;

			@Override
			public String translate(String arg0, String arg1) throws Exception {
				// TODO Auto-generated method stub
				return null;
			}
		});
		
				// translate vertex IDs, edge IDs, vertex values, and edge values to LongValue
//			Graph<LongValue, LongValue, LongValue> updatedGraph3 = graph
//			.translateGraphIds(new LongToLongValue())
//			.translateVertexValues(new LongToLongValue())
//			.translateEdgeValues(new LongToLongValue()) ；
				
	}
}
