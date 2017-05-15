package state;

import java.io.IOException;

import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class StateBackend {

	public static void main(String[] args) throws IOException {
		
		//MemoryStateBackend
//		MemoryStateBacked 将数据以对象的形式存储在Java的堆内存中。基于 Key/Value 的状态以及窗口操作的算子内部会有一个哈希表，用于保存各种值。 在做 checkpoint 的时候，状态后端会对状态做快照，然后将其作为 checkpoint 确认消息的一部分发送到 master 的 JobManager，JobManager也会将其保存在堆内存中。
//
//		MemoryStateBackend的限制：
//
//		每个状态的大小默认被限制为5MB，该值可以在 MemoryStateBackend 的构造函数中进行调整
//		无论配置的最大状态是多少，最终都不能超过 akka 的 最大frame 大小（见[Configuration](/setup/config.html）
//		聚合后的状态必须能够放进 JobManager 的内存中
//		以下场景推荐使用 MemoryStateBackend：
//
//		本地开发和调试
//		只需要保存很少状态的 Job，如那些只由每次只生成一条记录的算子（如Map、FlatMap、Filter等）组合而成的 Job。Kafka Consumer 也只需要保存很少的状态。
		
//	    FsStateBackend 通过一个文件系统的 URL 来配置，如 “hdfs://namenode:40010/flink/checkpoints” 或 “file:///data/flink/checkpoints”。
//
//		FsStateBackend 将运行时的数据保存在 TaskManager 的内存中。在做 checkpoint的时候，它会将状态快照存储到配置的文件系统目录中。 JobManager 的内存中仍然保存了少量的元数据（在高可用模式下，元数据会存储在对应的元数据 checkpoint中）。
//
//		以下场景推荐使用 FsStateBackend： * 拥有很大状态、较长的窗口时间或较大的 Key/Value 状态的 Job * 需要高可用的情况
//
//		RocksDBStateBackend
//		RocksDBStateBackend 也通过一个文件系统 URL 来配置，如 “hdfs://namenode:40010/flink/checkpoints” 或 “file:///data/flink/checkpoints”。
//
//		RocksDBStateBackend 将运行时的数据保存在 RocksDB 中，其中 RocksDB 的数据默认存储在 TaskManager 的数据目录中。 在做 checkpoint 的时候，RocksDb 中的完整数据将会被存储到目标的文件系统中。 JobManager 的内存中仍然保存了少量的元数据（在高可用模式下，元数据会存储在对应的元数据 checkpoint中）。
//
//		以下场景推荐使用 RocksDBStateBackend： * 拥有很大状态、较长的窗口时间或较大的 Key/Value 状态的 Job * 需要高可用的情况
//
//		注意此时能够保存的状态大小仅受限于可用磁盘空间，与 FsStateBackend 需要将运行时的数据保存在内存中相比， 通过 RocksDBStateBackend 你可以保存非常大的状态。然而这也可能会降低最大吞吐量。
//
//		注： 要使用 RocksDBStateBackend，还需要在你的项目中加入以下 maven 依赖：
//
//		<dependency>
//		  <groupId>org.apache.flink</groupId>
//		  <artifactId>flink-statebackend-rocksdb_2.10</artifactId>
//		  <version>1.1-SNAPSHOT</version>
//		</dependency>
//		RocksDBStateBackend 目前并没有被包含在 Flink 的二进制分发包中， 参见这里了解如何引入 RocksDBStateBackend 并在集群中使用它。
		
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setStateBackend(new FsStateBackend("hdfs://namenode:40010/flink/checkpoints"));
 		
		
//		配置默认的状态后端
//		可以在 flink-conf.yaml 中使用 state.backend 配置项来配置默认的状态后端。
//		这个配置的值可以是：jobmanager （表示使用 MemoryStateBackend）， filesystem （使用 FsStateBackend）， 或使用实现了状态后端工厂 FsStateBackendFactory 的全限定类名。
//		当默认的状态后端被设置为 filesystem 时，配置项 state.backend.fs.checkpointdir 指定了 checkpoint 数据的存储目录。
//		示例配置如下：
//		# The backend that will be used to store operator state checkpoints
//		state.backend: filesystem
//		# Directory for storing checkpoints
//		state.backend.fs.checkpointdir: hdfs://namenode:40010/flink/checkpoints
		
	}
}
