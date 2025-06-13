package gr.edu.flink.model;

import java.time.LocalDateTime;

public record DeltaPriceReport(
    LocalDateTime timestamp,
    Double change,
    Double previousWindowMaxPrice,
    Double currentWindowMaxPrice
) {

  @Override
  public String toString(){
    return String.format("Large Change Detected of %s (%s - %s) at %s",
        change, previousWindowMaxPrice, currentWindowMaxPrice, timestamp);
  }
}
