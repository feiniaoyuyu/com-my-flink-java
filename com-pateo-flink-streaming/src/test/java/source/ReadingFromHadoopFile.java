package source;

import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

public class ReadingFromHadoopFile {

	public static void main(String[] args) throws Exception {

		final ExecutionEnvironment env = ExecutionEnvironment
				.getExecutionEnvironment();

		String inputPath = "/tmp/wl_markfile.log";
		org.apache.flink.api.java.hadoop.mapreduce.HadoopInputFormat<LongWritable, Text> hadoopFile = 
				org.apache.flink.hadoopcompatibility.HadoopInputs
				.readHadoopFile(
						new org.apache.hadoop.mapreduce.lib.input.TextInputFormat(), // extends
																						// org.apache.hadoop.mapreduce.lib.input.FileInputFormat
						org.apache.hadoop.io.LongWritable.class,
						org.apache.hadoop.io.Text.class,

						inputPath);
		DataSource<Tuple2<LongWritable, Text>> createInput = env.createInput(hadoopFile);
		createInput.print();
//		OutputFormat mapreduceOutputFormat;
//		Job job;
//		HadoopOutputFormat outputFormat = new HadoopOutputFormat(mapreduceOutputFormat, job);
		//String filePath = "/tmp/wl_markfile.log2";
		//createInput.write(outputFormat, filePath);
	}
	
}
//import org.apache.hadoop.fs.Path
//import org.apache.hadoop.io.SequenceFile.CompressionType
//import org.apache.hadoop.io.{Text, LongWritable}
//import org.apache.hadoop.mapred.{FileOutputFormat, JobConf, TextOutputFormat, TextInputFormat}
//imoprt org.apache.flink.api.scala.hadoop.mapred.HadoopOutputFormat
// 
//val benv = ExecutionEnvironment.getExecutionEnvironment
//val input = benv.readHadoopFile(new TextInputFormat, classOf[LongWritable], classOf[Text], textPath)
// 
//val text = input map { _._2.toString }
//val counts = text.flatMap { _.toLowerCase.split("\\W+") filter { _.nonEmpty } }
//  .map { (_, 1) }
//  .groupBy(0)
//  .sum(1)
// 
//val words = counts map { t => (new Text(t._1), new LongWritable(t._2)) }
// 
//val hadoopOutputFormat = new HadoopOutputFormat[Text,LongWritable](
//  new TextOutputFormat[Text, LongWritable], new JobConf)
//val c = classOf[org.apache.hadoop.io.compress.GzipCodec]
//hadoopOutputFormat.getJobConf.set("mapred.textoutputformat.separator", " ")
//hadoopOutputFormat.getJobConf.setCompressMapOutput(true)
//hadoopOutputFormat.getJobConf.set("mapred.output.compress", "true")
//hadoopOutputFormat.getJobConf.setMapOutputCompressorClass(c)
//hadoopOutputFormat.getJobConf.set("mapred.output.compression.codec", c.getCanonicalName)
//hadoopOutputFormat.getJobConf.set("mapred.output.compression.type", CompressionType.BLOCK.toString)
// 
//FileOutputFormat.setOutputPath(hadoopOutputFormat.getJobConf, new Path("/tmp/iteblog/"))
// 
//words.output(hadoopOutputFormat)
//benv.execute("Hadoop Compat WordCount")
