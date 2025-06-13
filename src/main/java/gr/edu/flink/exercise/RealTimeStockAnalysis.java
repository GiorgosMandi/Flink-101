package gr.edu.flink.exercise;

import static gr.edu.flink.util.Constants.SOCKET_PORT;

import gr.edu.flink.model.DeltaPriceReport;
import gr.edu.flink.model.StockReport;
import gr.edu.flink.model.Trade;
import gr.edu.flink.util.MyFunctions;
import java.io.IOException;
import java.time.Duration;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringEncoder;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.file.sink.FileSink;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.DefaultRollingPolicy;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

public class RealTimeStockAnalysis {

  /**
   * For every minute compute:
   *  a) Maximum trade price
   *  b) Minimum trade price
   *  c) Maximum trade volume
   *  d) Minimum trade volume
   *  e) % change in max trade volume form previous minute
   *  f) % change in max trade volume form previous minute
   *  And
   *  For every 5-minute window, if the max trade price is changing more than 5%,
   *  then record the event
   *
   * @param args input args
   * @throws Exception in case of error
   */
  public static void main(String[] args) throws Exception {

    final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

    // sink for stock analysis
    var outputPath = "src/main/resources/outputs/stock-report";
    FileSink<String> sink = FileSink
        .forRowFormat(new Path(outputPath), new SimpleStringEncoder<String>("UTF-8"))
        .withRollingPolicy(
            DefaultRollingPolicy.builder()
                .withRolloverInterval(Duration.ofMinutes(15))
                .withInactivityInterval(Duration.ofMinutes(5))
                .build()
        )
        .build();

    // sink for large delta changes report
    var outputPath2 = "src/main/resources/outputs/large-delta-report";
    FileSink<String> largeDeltaSink = FileSink
        .forRowFormat(new Path(outputPath2), new SimpleStringEncoder<String>("UTF-8"))
        .withRollingPolicy(
            DefaultRollingPolicy.builder()
                .withRolloverInterval(Duration.ofMinutes(15))
                .withInactivityInterval(Duration.ofMinutes(5))
                .build()
        )
        .build();

    WatermarkStrategy<Trade> ws = WatermarkStrategy.<Trade>forMonotonousTimestamps()
        .withTimestampAssigner(
            (trade, timestamp) -> trade.timestamp().toInstant(ZoneOffset.UTC).toEpochMilli()
        );

    var trades = env.socketTextStream("localhost", SOCKET_PORT)
        .map(new MyFunctions.TradeParser())
        .assignTimestampsAndWatermarks(ws)
        .keyBy(Trade::stock);

    // trades minutely analysis
    trades.window(TumblingEventTimeWindows.of(Duration.ofMinutes(1)))
        .process(new TradeWindowAnalysis())
        .map(StockReport::toString)
        .sinkTo(sink);

    // trades 5 minutely checks for large price changes
    trades.window(TumblingEventTimeWindows.of(Duration.ofMinutes(5)))
        .process(new TrackLargeDelta(5))
        .map(DeltaPriceReport::toString)
        .sinkTo(largeDeltaSink);

    // execute
    env.execute("Stock analysis");
  }


  public static class TradeWindowAnalysis extends
      ProcessWindowFunction<Trade, StockReport, String, TimeWindow> {
    private transient ValueState<Double> prevWindowMaxTrade;
    private transient ValueState<Integer> prevWindowMaxVol;

    // process whole window
    @Override
    public void process(String key,
                        Context context,
                        Iterable<Trade> trades,
                        Collector<StockReport> out
    ) throws IOException {
      LocalDateTime windowStart = null;
      LocalDateTime windowEnd = null;
      Double windowMaxPrice = Double.MIN_NORMAL;
      Double windowMinPrice = Double.MAX_VALUE;
      Integer windowMaxVol = Integer.MIN_VALUE;
      Integer windowMinVol = Integer.MAX_VALUE;


      // iterate over window's items
      for (Trade trade : trades) {
        if (windowStart == null) {
          windowStart = trade.timestamp();
          windowMinPrice = trade.price();
          windowMinVol = trade.volume();
        }
        if (trade.price() > windowMaxPrice) {
          windowMaxPrice = trade.price();
        }

        if (trade.price() < windowMinPrice) {
          windowMinPrice = trade.price();
        }

        if (trade.volume() > windowMaxVol) {
          windowMaxVol = trade.volume();
        }
        if (trade.volume() < windowMinVol) {
          windowMinVol = trade.volume();
        }
        windowEnd = trade.timestamp();
      }

      double maxPriceChange = 0.0;
      double maxVolChange = 0.0;

      Double prevTrade = prevWindowMaxTrade.value();
      if (prevTrade != null) {
        maxPriceChange = ((windowMaxPrice - prevTrade) / prevTrade) * 100;
      }
      Integer prevVol = prevWindowMaxVol.value();
      if (prevVol != null) {
        maxVolChange = ((windowMaxVol - prevVol) * 1.0 / prevVol) * 100;
      }

      var sr = new StockReport(windowStart, windowEnd, windowMaxPrice, windowMinPrice, windowMaxVol,
          windowMinVol, maxPriceChange, maxVolChange);
      out.collect(sr);

      prevWindowMaxTrade.update(windowMaxPrice);
      prevWindowMaxVol.update(windowMaxVol);
    }

    @Override
    public void open(Configuration config) {

      ValueStateDescriptor<Double> tradeDesc =
          new ValueStateDescriptor<>("prev_max_trade", Types.DOUBLE);
      prevWindowMaxTrade = getRuntimeContext().getState(tradeDesc);

      ValueStateDescriptor<Integer> volDesc =
          new ValueStateDescriptor<>("prev_max_vol", Types.INT);
      prevWindowMaxVol = getRuntimeContext().getState(volDesc);
    }
  }


  public static class TrackLargeDelta extends
      ProcessWindowFunction<Trade, DeltaPriceReport, String, TimeWindow> {

    private final double threshold;
    private transient ValueState<Double> prevWindowMaxTrade;

    public TrackLargeDelta(double threshold) {
      this.threshold = threshold;
    }

    @Override
    public void process(String key,
                        Context context,
                        Iterable<Trade> trades,
                        Collector<DeltaPriceReport> out
    ) throws IOException {

      var prevMax = 0.0;
      if (prevWindowMaxTrade.value() != null) {
        prevMax = prevWindowMaxTrade.value();
      }
      var currMax = 0.0;
      var currMaxTimestamp = LocalDateTime.now();
      for (Trade trade : trades) {

        if (trade.price() > currMax) {
          currMax = trade.price();
          currMaxTimestamp = trade.timestamp();
          prevWindowMaxTrade.update(currMax);

        }

        // check if change is more than the specified threshold
        var maxTradePriceChange = ((currMax - prevMax) / prevMax) * 100;

        if (prevMax != 0 && Math.abs(maxTradePriceChange) > threshold) {
          var dpr = new DeltaPriceReport(currMaxTimestamp, maxTradePriceChange,  prevMax, currMax);
          out.collect(dpr);
        }
      }
    }

    @Override
    public void open(Configuration config) {
      ValueStateDescriptor<Double> tradeDesc =
          new ValueStateDescriptor<>("prev_max_trade", Types.DOUBLE);
      prevWindowMaxTrade = getRuntimeContext().getState(tradeDesc);
    }
  }
}
