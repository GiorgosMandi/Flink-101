package gr.edu.flink.model;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

public record Trade(LocalDateTime timestamp, String stock, Double price, Integer volume) {
  public Trade(String date, String time, String price, String volume) {
    this(
        LocalDateTime.parse(date + " " + time, DateTimeFormatter.ofPattern("dd/MM/yyyy HH:mm:ss")),
        "DEFAULT",
        Double.parseDouble(price),
        Integer.parseInt(volume)
    );
  }
}
