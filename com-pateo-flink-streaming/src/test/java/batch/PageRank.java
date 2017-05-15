package batch;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatJoinFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.IterativeDataSet;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

public class PageRank {

	private static final String DAMPENING_FACTOR = null;

	public static void main(String[] args) {
		ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

		// read the pages and initial ranks by parsing a CSV file
		DataSet<Tuple2<Long, Double>> pagesWithRanks = env.readCsvFile("pagesInputPath")
								   .types(Long.class, Double.class) ;

		// the links are encoded as an adjacency list: (page-id, Array(neighbor-ids))
		DataSet<Tuple2<Long, Long[]>> pageLinkLists = getLinksDataSet(env);

		int maxIterations = 100;
		// set iterative data set
		IterativeDataSet<Tuple2<Long, Double>> iteration = pagesWithRanks.iterate(maxIterations );

		Object numPages;
		DataSet<Tuple2<Long, Double>> newRanks = iteration
		        // join pages with outgoing edges and distribute rank
		        .join(pageLinkLists).where(0).equalTo(0).flatMap( new FlatMapFunction<Tuple2<Tuple2<Long,Double>,Tuple2<Long,Long[]>>, Tuple2<Long,Double>>() {
 
					private static final long serialVersionUID = 1L;

					@Override
					public void flatMap(
							Tuple2<Tuple2<Long, Double>, Tuple2<Long, Long[]>> value,
							Collector<Tuple2<Long, Double>> out)
							throws Exception {
 						
					}
				})
		        // collect and sum ranks
		        .groupBy(0).sum(1)
		        // apply dampening factor  DAMPENING_FACTOR, numPages
		        .map(new MapFunction<Tuple2<Long,Double>,  Tuple2<Long,Double>>() {
 
					private static final long serialVersionUID = 1L;

					@Override
					public Tuple2<Long, Double> map(Tuple2<Long, Double> value)
							throws Exception {
 
						return null;
					}
				});

		DataSet<Tuple2<Long, Double>> finalPageRanks = iteration.closeWith(
		        newRanks,
		        newRanks.join(iteration).where(0).equalTo(0)
		        // termination condition
		        .filter(new EpsilonFilter()));

		String outputPath = null;
		finalPageRanks.writeAsCsv(outputPath, "\n", " ");
	
	}

	// User-defined functions ================================== 
	
	private static final DataSet<Tuple2<Long, Long[]>> getLinksDataSet(
			ExecutionEnvironment env) {
 		return null;
	}
	 
 
	static final  class JoinVertexWithEdgesMatch 
	                    implements FlatMapFunction<Tuple2<Long, Double>, Tuple2<Long, Double>> ,FlatJoinFunction<Tuple2<Long, Double>, Tuple2<Long, Long[]>,Tuple2<Long, Double>> {

		private static final long serialVersionUID = 1L;

		@Override
	    public void join( Tuple2<Long, Double> page, Tuple2<Long, Long[]> adj,
	                        Collector<Tuple2<Long, Double>> out) {
	        Long[] neighbors = adj.f1;
	        double rank = page.f1;
	        String neigbors = "" ;
			double rankToDistribute = rank / ((double) neigbors.length());

	        for (int i = 0; i < neighbors.length; i++) {
	            out.collect(new Tuple2<Long, Double>(neighbors[i], rankToDistribute));
	        }
	    }

		@Override
		public void flatMap(Tuple2<Long, Double> value,
				Collector<Tuple2<Long, Double>> out) throws Exception {
 			
		}

	}

	static final class Dampener 
			implements MapFunction<Tuple2<Long,Double>, Tuple2<Long,Double>> {
	 
		private static final long serialVersionUID = 1L;
		private final double dampening, randomJump;

	    public Dampener(double dampening, double numVertices) {
	        this.dampening = dampening;
	        this.randomJump = (1 - dampening) / numVertices;
	    }

	    @Override
	    public Tuple2<Long, Double> map(Tuple2<Long, Double> value) {
	        value.f1 = (value.f1 * dampening) + randomJump;
	        return value;
	    }
	}

 	static final class EpsilonFilter
	                implements FilterFunction<Tuple2<Tuple2<Long, Double>, Tuple2<Long, Double>>> {

		private static final long serialVersionUID = 1L;
		private static final double EPSILON = 0;

		@Override
	    public boolean filter(Tuple2<Tuple2<Long, Double>, Tuple2<Long, Double>> value) {
	        return Math.abs(value.f0.f1 - value.f1.f1) > EPSILON;
	    }
	}
}
