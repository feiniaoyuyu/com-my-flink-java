package asynio;

import java.io.IOException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.RunnableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.apache.flink.streaming.api.datastream.AsyncDataStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.async.AsyncFunction;
import org.apache.flink.streaming.api.functions.async.collector.AsyncCollector;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;
import org.hbase.async.HBaseClient;


/***  ***/
class UserCallback {
	UserCallback (AsyncCollector<String> c){
	}
}
class HBaseAsyncFunction2 implements AsyncFunction<String, String> {
	 
	private static final long serialVersionUID = 1L;
	// initialize it while reading object
	transient Connection connection;
	@Override
	public void asyncInvoke(String val, AsyncCollector<String> c) throws IllegalArgumentException, IOException {
	Get get = new Get(Bytes.toBytes(val));
	Table ht = connection.getTable(TableName.valueOf(Bytes.toBytes("test"))); ;
	
	// UserCallback is from user’s async client.
	
//((AsyncableHTableInterface) ht).asyncGet(get, new UserCallback(c));
	new HBaseClient("");
}
  
// create data stream
public void createHBaseAsyncTestStream(StreamExecutionEnvironment env) {
	DataStream<String> source = getDataStream(env);
//	DataStream<String> stream = AsyncDataStream.unorderedWait(source, new HBaseAsyncFunction());
//	stream.print();
}

private DataStream<String> getDataStream(StreamExecutionEnvironment env) {
 	return null;
}
}

//public class AsynioTest {
//
//	
//	public static void main(String[] args) {
//		
//		
////A DirectExecutor can be obtained via org.apache.flink.runtime.concurrent.Executors.directExecutor() or com.google.common.util.concurrent.MoreExecutors.directExecutor().
//		Executor directExecutor = org.apache.flink.runtime.concurrent.Executors.directExecutor();
//
//		directExecutor.execute(new Runnable() {
//			
//			@Override
//			public void run() {
//				
//			}
//		});
//		
//		directExecutor.execute(new RunnableFuture<String>() {
//
//			@Override
//			public boolean cancel(boolean mayInterruptIfRunning) {
//				
//				return false;
//			}
//
//			@Override
//			public boolean isCancelled() {
// 				return false;
//			}
//
//			@Override
//			public boolean isDone() {
// 				return false;
//			}
//
//			@Override
//			public String get() throws InterruptedException, ExecutionException {
// 				return null;
//			}
//
//			@Override
//			public String get(long timeout, TimeUnit unit)
//					throws InterruptedException, ExecutionException,
//					TimeoutException {
// 				return null;
//			}
//
//			@Override
//			public void run() {
// 				
//			}
//		});
//		
//		//com.google.common.util.concurrent.MoreExecutors.getExitingExecutorService(executor);
//		
//	}
//}
