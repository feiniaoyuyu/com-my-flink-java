package asynio;

//import com.google.common.util.concurrent.AsyncFunction;
//import org.apache.flink.streaming.api.functions.async.AsyncFunction;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.apache.flink.shaded.com.google.common.util.concurrent.AsyncFunction;
import org.apache.flink.streaming.api.datastream.AsyncDataStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.async.RichAsyncFunction;
import org.apache.flink.streaming.api.functions.async.collector.AsyncCollector;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;

import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.common.util.concurrent.ListenableFuture;

public class HBaseAsyncFunction
		implements
		org.apache.flink.streaming.api.functions.async.AsyncFunction<String, String> {

	// initialize it while reading object
	private static final long serialVersionUID = 1L;
	transient Connection connection;

	@Override
	public void asyncInvoke(String val, AsyncCollector<String> c)
			throws IllegalArgumentException, IOException {
		Get get = new Get(Bytes.toBytes(val));
		Table ht = connection
				.getTable(TableName.valueOf(Bytes.toBytes("test")));

		ListenableFuture<Result> future = null; // 
		//ht.asyncGet(get);
		Futures.addCallback(future, new FutureCallback<Result>() {
			@Override
			public void onSuccess(Result result) {
				List<String> ret = new ArrayList<String>();
				ret.add(new String(result.getRow()));
				c.collect(ret);
			}

			@Override
			public void onFailure(Throwable t) {
				c.collect(t);
			}
		}, MoreExecutors.sameThreadExecutor()); //newDirectExecutorService()
	}

	// create data stream
	public void createHBaseAsyncTestStream(StreamExecutionEnvironment env) {
		DataStream<String> source = getDataStream(env);
		DataStream<String> stream = AsyncDataStream.unorderedWait(source,
				new HBaseAsyncFunction(), 2000, TimeUnit.MILLISECONDS, 10);
		stream.print();
	}

	private DataStream<String> getDataStream(StreamExecutionEnvironment env) {
		return null;
	}

	public org.apache.flink.shaded.com.google.common.util.concurrent.ListenableFuture<String> apply(
			String arg0) throws Exception {
		return null;
	}

}