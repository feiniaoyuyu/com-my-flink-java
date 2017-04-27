package org.apache.flink.streaming.examples.windowing;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.core.fs.FileSystem.WriteMode;
import org.apache.flink.examples.java.wordcount.util.WordCountData;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import flink.java.WordCount;

public class WindowWordCount {

	// window parameters with default values
	private static int windowSize = 250;
	private static int slideSize = 150;

	// *************************************************************************
	// PROGRAM
	// *************************************************************************

	public static void main(String[] args) throws Exception {

		if (!parseParameters(args)) {
			return;
		}

		// set up the execution environment
		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

		// get input data
		DataStream<String> text = getTextDataStream(env);
		text.print();
		
		DataStream<Tuple2<String, Integer>> counts =
		// split up the lines in pairs (2-tuples) containing: (word,1)
		text.flatMap(new WordCount.Tokenizer())
				// create windows of windowSize records slided every slideSize records
				.keyBy(0)
				.countWindow(windowSize, slideSize)
				// group by the tuple field "0" and sum up tuple field "1"
				.sum(1);
		
		counts.print();
		
		// emit result
		if (fileOutput) {
			counts.writeAsText(outputPath, WriteMode.OVERWRITE);
		} else {
			counts.print();
		}

		// execute program
		env.execute("WindowWordCount");
	}


	// *************************************************************************
	// UTIL METHODS
	// *************************************************************************

	private static boolean fileOutput = false;
	private static String textPath;
	private static String outputPath;

	private static boolean parseParameters(String[] args) {

		if (args.length > 0) {
			// parse input arguments
			fileOutput = true;
			if (args.length >= 2 && args.length <= 4) {
				textPath = args[0];
				outputPath = args[1];
				if (args.length >= 3){
					windowSize = Integer.parseInt(args[2]);

					// if no slide size is specified use the
					slideSize = args.length == 3 ? windowSize : Integer.parseInt(args[2]);
				}
			} else {
				System.err.println("Usage: WindowWordCount <text path> <result path> [<window size>] [<slide size>]");
				return false;
			}
		} else {
			System.out.println("Executing WindowWordCount example with built-in default data.");
			System.out.println("  Provide parameters to read input data from a file.");
			System.out.println("  Usage: WindowWordCount <text path> <result path> [<window size>] [<slide size>]");
		}
		return true;
	}

	private static DataStream<String> getTextDataStream(StreamExecutionEnvironment env) {
		if (fileOutput) {
			// read the text file from given input path
			return env.readTextFile(textPath);
		} else {
			// get default test text data
			return env.fromElements(WordCountData.WORDS);
		}
	}
}