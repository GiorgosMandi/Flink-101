package gr.edu.flink.state;

import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.serialization.SimpleStringEncoder;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink;
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.DefaultRollingPolicy;
import org.apache.flink.util.Collector;

public class ListStateExample {

  public static void main(String[] args) throws Exception {

    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    var sum = env.socketTextStream("localhost", 9999)
        .map(l -> {
          var parts = l.split(",");
          return new Tuple2<>(Long.parseLong(parts[0]), parts[1]);
        })
        .returns(Types.TUPLE(Types.LONG, Types.STRING))
        .keyBy(t -> t.f0)
        .flatMap(new StatefulMap());

    sum.addSink(
        StreamingFileSink
            .forRowFormat(
                new Path("src/main/resources/datasets/outputs/list-state"),
                new SimpleStringEncoder<Tuple2<String, Long>>("UTF-8")
            )
            .withRollingPolicy(DefaultRollingPolicy.builder().build())
            .build()
    );

    // execute program
    env.execute("State");

  }


  public static class StatefulMap
      extends RichFlatMapFunction<Tuple2<Long, String>, Tuple2<String, Long>> {
    private transient ValueState<Long> count;
    private transient ListState<Long> numbers;

    public void flatMap(Tuple2<Long, String> input, Collector<Tuple2<String, Long>> out)
        throws Exception {
      Long currValue = Long.parseLong(input.f1);
      Long currCount = 0L;

      if (count.value() != null) {
        currCount = count.value();
      }

      currCount += 1;

      count.update(currCount);
      numbers.add(currValue);

      if (currCount >= 10) {
        Long sum = 0L;
        String numbersStr = "";
        for (Long number : numbers.get()) {
          numbersStr = numbersStr + " " + number;
          sum = sum + number;
        }
        /* emit sum of last 10 elements */
        out.collect(Tuple2.of(numbersStr, sum));
        /* clear value */
        count.clear();
        numbers.clear();
      }
    }

    public void open(Configuration conf) {
      var listDesc = new ListStateDescriptor<>("numbers", Long.class);
      numbers = getRuntimeContext().getListState(listDesc);

      var descriptor2 = new ValueStateDescriptor<>("count", Long.class);
      count = getRuntimeContext().getState(descriptor2);
    }
  }
}
