package gr.edu.flink.state;

import static gr.edu.flink.util.Constants.SOCKET_PORT;

import java.time.Duration;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.serialization.SimpleStringEncoder;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.file.sink.FileSink;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.DefaultRollingPolicy;
import org.apache.flink.util.Collector;

public class ListStateExample {

  public static void main(String[] args) throws Exception {

    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

    var outputPath = "src/main/resources/outputs/list-state";
    FileSink<String> sink = FileSink
        .forRowFormat(
            new Path(outputPath),
            new SimpleStringEncoder<String>("UTF-8")
        )
        .withRollingPolicy(
            DefaultRollingPolicy.builder()
                .withRolloverInterval(Duration.ofMinutes(15))
                .withInactivityInterval(Duration.ofMinutes(5))
                .build()
        )
        .build();

    env.socketTextStream("localhost", SOCKET_PORT)
        .map(l -> {
          var parts = l.split(",");
          return new Tuple2<>(Long.parseLong(parts[0]), parts[1]);
        })
        .returns(Types.TUPLE(Types.LONG, Types.STRING))
        .keyBy(t -> t.f0)
        .flatMap(new StatefulMap())
        .map(Object::toString)
        .sinkTo(sink);

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
        long sum = 0L;
        StringBuilder numbersStr = new StringBuilder();
        for (Long number : numbers.get()) {
          numbersStr.append(" ").append(number);
          sum = sum + number;
        }
        /* emit sum of last 10 elements */
        out.collect(Tuple2.of(numbersStr.toString(), sum));
        /* clear value */
        count.clear();
        numbers.clear();
      }
    }

    @Override
    public void open(Configuration conf) {
      var listDesc = new ListStateDescriptor<>("numbers", Long.class);
      numbers = getRuntimeContext().getListState(listDesc);

      var descriptor2 = new ValueStateDescriptor<>("count", Long.class);
      count = getRuntimeContext().getState(descriptor2);
    }
  }
}
