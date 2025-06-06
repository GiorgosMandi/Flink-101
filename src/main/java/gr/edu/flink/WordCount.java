package gr.edu.flink;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

public class WordCount {

  public static void main(String[] args) throws Exception {

    final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

    // Source: Read text from socket
    DataStream<String> text = env.socketTextStream("localhost", 9999);

    // Process: Split into words, map to (word, 1), and sum counts
    DataStream<Tuple2<String, Integer>> wordCounts = text
        .flatMap(new Tokenizer())
        .keyBy(value -> value.f0)
        .sum(1);

    // Sink: Print to stdout
    wordCounts.print();

    // Execute the job
    env.execute("Flink 2.0 Java WordCount");
  }

  // User-defined FlatMapFunction
  public static class Tokenizer implements FlatMapFunction<String, Tuple2<String, Integer>> {
    @Override
    public void flatMap(String value, Collector<Tuple2<String, Integer>> out) {
      for (String word : value.toLowerCase().split("\\W+")) {
        if (!word.isEmpty()) {
          out.collect(new Tuple2<>(word, 1));
        }
      }
    }
  }
}