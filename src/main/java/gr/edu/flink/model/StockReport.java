package gr.edu.flink.model;

import java.time.LocalDateTime;

public record StockReport(
    LocalDateTime windowStart,
    LocalDateTime windowEnd,
    Double maxPrice,
    Double minPrice,
    Integer maxVolume,
    Integer minVolume,
    Double maxPriceChange,
    Double maxVolChange
) {


  @Override
  public String toString(){
    return String.format("%s-%s\t%s\t%s\t%s\t%s\t%s\t%s",
        windowStart, windowEnd, maxPrice, minPrice, maxVolume, minVolume, maxPriceChange,
        maxVolChange);
  }
}
