package gr.edu.flink.model;

import java.time.LocalDate;
import java.time.format.DateTimeFormatter;

public record Purchase(LocalDate date, String month, String category, String product, int amount) {

  public Purchase(String date, String month, String category, String product, int amount) {
    this(
        LocalDate.parse(date, DateTimeFormatter.ofPattern("dd-MM-yyyy")),
        month,
        category,
        product,
        amount
    );
  }
}
