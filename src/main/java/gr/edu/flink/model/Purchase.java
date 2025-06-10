package gr.edu.flink.model;

import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import lombok.AllArgsConstructor;
import lombok.Data;

@Data
@AllArgsConstructor
public class Purchase {

  private LocalDate date;
  private String month;
  private String category;
  private String product;
  private int amount;

  public Purchase(String date, String month, String category, String product, int amount) {
    var formatter = DateTimeFormatter.ofPattern("dd-MM-yyyy");
    this.date = LocalDate.parse(date, formatter);
    this.month = month;
    this.category = category;
    this.product = product;
    this.amount = amount;
  }

  @Override
  public String toString() {
    return String.format("Purchase(%s, %s, %s)", product, month, amount);
  }
}
