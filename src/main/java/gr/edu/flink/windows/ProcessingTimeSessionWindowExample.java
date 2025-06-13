package gr.edu.flink.windows;

import static gr.edu.flink.util.Constants.SOCKET_PORT;

import gr.edu.flink.util.MyFunctions;
import java.time.Duration;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.ProcessingTimeSessionWindows;

public class ProcessingTimeSessionWindowExample {

  public static void main(String[] args) throws Exception {

    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

    env.socketTextStream("localhost", SOCKET_PORT)
        .map(new MyFunctions.PurchaseParser())
        .map(p -> Tuple3.of(p.month(), p.amount(), 0))
        .returns(Types.TUPLE(Types.STRING, Types.INT, Types.INT))
        .keyBy(t -> t.f0)
        .window(ProcessingTimeSessionWindows.withGap(Duration.ofSeconds(1)))
        .reduce(new MyFunctions.AmountReducer())
        .print();

    env.execute("Avg Profit Per Month, streamed from socket");
  }
}