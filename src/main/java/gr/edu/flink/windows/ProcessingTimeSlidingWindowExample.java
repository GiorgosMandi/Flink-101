package gr.edu.flink.windows;

import gr.edu.flink.util.MyFunctions;
import java.time.Duration;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;

public class ProcessingTimeSlidingWindowExample {

  public static void main(String[] args) throws Exception {

    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

    env.socketTextStream("localhost", 9999)
        .map(new MyFunctions.PurchaseParser())
        .map(p -> Tuple3.of(p.getMonth(), p.getAmount(), 0))
        .returns(Types.TUPLE(Types.STRING, Types.INT, Types.INT))
        .keyBy(t -> t.f0)
        .window(SlidingEventTimeWindows.of(Duration.ofSeconds(2), Duration.ofSeconds(1)))
        .reduce(new MyFunctions.AmountReducer())
        .print();

    env.execute("Avg Profit Per Month, streamed from socket");
  }
}