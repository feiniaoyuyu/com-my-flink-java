package asynio;

import com.google.common.base.Charsets;
import com.stumbleupon.async.Callback;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.zookeeper.ZKConfig;
import org.hbase.async.ClientStats;
import org.hbase.async.GetRequest;
import org.hbase.async.HBaseClient;
import org.hbase.async.KeyValue;
import org.hbase.async.PutRequest;
import org.hbase.async.Scanner;

import java.util.ArrayList;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 *
 */
public class AsyncHBaseQuickStart {

	public static final String DEFAULT_ZK_DIR = "/hbase";

	// HBase Client
	private HBaseClient hBaseClient;
	private String tableName;
	private String columnFamily;
	private String zkQuorum;

	private Long start = System.currentTimeMillis();

	public AsyncHBaseQuickStart(String tableName, String columnFamily,
			String zkQuorum) {
		this.tableName = tableName;
		this.columnFamily = columnFamily;
		this.zkQuorum = zkQuorum;
	}

	/**
	 * Initialize the client and verify if Table and Cf exists
	 *
	 * @throws Exception
	 */
	public void init() throws Exception {
		if (zkQuorum == null || zkQuorum.isEmpty()) {
			// Follow the default path
			Configuration conf = HBaseConfiguration.create();
			// conf.addResource("/com-pateo-flink-streaming/resource/hbase-site.xml");

			zkQuorum = ZKConfig.getZKQuorumServersString(conf);
		}
		hBaseClient = new HBaseClient(zkQuorum, DEFAULT_ZK_DIR,
				Executors.newCachedThreadPool());
		//hBaseClient.delete(request) ;
		// Lets ensure that Table and Cf exits
		final CountDownLatch latch = new CountDownLatch(1);
		final AtomicBoolean fail = new AtomicBoolean(false);
		hBaseClient.ensureTableFamilyExists(tableName, columnFamily)
				.addCallbacks(new Callback<Object, Object>() {
					@Override
					public Object call(Object arg) throws Exception {
						latch.countDown();
						return null;
					}
				}, new Callback<Object, Object>() {
					@Override
					public Object call(Object arg) throws Exception {
						fail.set(true);
						latch.countDown();
						return null;
					}
				});

		try {
			latch.await();
		} catch (InterruptedException e) {
			throw new Exception("Interrupted", e);
		}

		if (fail.get()) {
			throw new Exception("Table or Column Family doesn't exist");
		}
	}

	/**
	 * Puts the data in HBase
	 *
	 * @param data
	 *            Data to be inserted in HBase
	 */
	public void putData(byte[] rowKey, String col, String data)
			throws Exception {

		System.out.println("putData begain "
				+ (System.currentTimeMillis() - start));

		PutRequest putRequest = new PutRequest(
				tableName.getBytes(Charsets.UTF_8), rowKey,
				columnFamily.getBytes(Charsets.UTF_8),
				col.getBytes(Charsets.UTF_8), data.getBytes(Charsets.UTF_8));
		final CountDownLatch latch = new CountDownLatch(1);

		final AtomicBoolean fail = new AtomicBoolean(false);
		
		hBaseClient.put(putRequest).addCallbacks(
				new Callback<Object, Object>() {
					@Override
					public Object call(Object arg) throws Exception {
						latch.countDown();
						System.out.println("countDown c "
								+ (System.currentTimeMillis() - start));
						return null;
					}
				}, new Callback<Object, Exception>() {
					@Override
					public Object call(Exception arg) throws Exception {
						fail.set(true);
						latch.countDown();
						System.out.println("countDown error "
								+ (System.currentTimeMillis() - start));

						return null;
					}
				});

		try {
			System.out.println("shutdown before " + (System.currentTimeMillis() - start));
			ClientStats stats = hBaseClient.stats();
			System.out.println("stats stats " + stats);
			hBaseClient.shutdown();
			System.out.println("countDown await " + (System.currentTimeMillis() - start));
			latch.await();
			hBaseClient.shutdown();

		} catch (InterruptedException e) {
			throw new Exception("Interrupted", e);
		}

		if (fail.get()) {
			System.out.println("fail " + (System.currentTimeMillis() - start));

			throw new Exception("put request failed");
		}
	}

	public byte[] scan(byte[] rowKey) throws Exception {
		Scanner newScanner = hBaseClient.newScanner(tableName);
		newScanner.setServerBlockCache(false);
		newScanner.setFamilies(columnFamily);
		newScanner.setQualifier("name");
		//newScanner.setStartKey(start_key);
		
		GetRequest getRequest = new GetRequest(tableName, rowKey);
		ArrayList<KeyValue> kvs = hBaseClient.get(getRequest).join();
		System.out.println("getData " + (System.currentTimeMillis() - start));

		return kvs.get(0).value();
	}

	public byte[] getData(byte[] rowKey) throws Exception {
		GetRequest getRequest = new GetRequest(tableName, rowKey);
		ArrayList<KeyValue> kvs = hBaseClient.get(getRequest).join();
		System.out.println("getData " + (System.currentTimeMillis() - start));

		return kvs.get(0).value();
	}

	public static void main(String[] args) throws Exception {
		System.out.println(UUID.randomUUID().toString());
		byte[] rowKey = UUID.randomUUID().toString().getBytes(Charsets.UTF_8);
		// byte[] rowKey = "row333".getBytes();
		AsyncHBaseQuickStart asyncHBaseQuickStart = new AsyncHBaseQuickStart(
				"usersfromhdfswithfilter", "cfInfo", "10.172.10.168:2181");
		asyncHBaseQuickStart.init();
		asyncHBaseQuickStart.putData(rowKey, "profile", "Sample Data #1");
		System.out.println(new String(asyncHBaseQuickStart.getData(rowKey)));

	}
}