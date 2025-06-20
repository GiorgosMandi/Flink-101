package gr.edu.flink;

import static gr.edu.flink.util.Constants.SOCKET_PORT;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.PrintWriter;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.Random;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class DataSocketEmitter {

  public static void main(String[] args) throws Exception {
    emitTrades();
  }


  private static void emitPurchases() throws Exception {
    var dataFilePath = "src/main/resources/datasets/purchases";
    log.info("Initializing socket to emit Purchases.");
    try (
        var listener = new ServerSocket(SOCKET_PORT);
        Socket socket = listener.accept()
    ) {
      log.info("Got new connection: {}. ", socket);
      BufferedReader br = new BufferedReader(new FileReader(dataFilePath));

      PrintWriter out = new PrintWriter(socket.getOutputStream(), true);
      String line;
      while ((line = br.readLine()) != null) {
        out.println(line);
        log.info("Sending -> '{}'. ", line);
      }
      log.info("Closing connection: {}. ", socket);
    }
  }

  private static void emitRandomNumbers() throws Exception {
    log.info("Initializing socket to emit Random Numbers.");
    var rand =  new Random();
    try (
        var listener = new ServerSocket(SOCKET_PORT);
        Socket socket = listener.accept()
    ) {
      log.info("Got new connection: {}. ", socket);
      PrintWriter out = new PrintWriter(socket.getOutputStream(), true);
      var i = 0;
      while (i != 100) {
        var timestamp = System.currentTimeMillis();
        var value = String.format("%s,%s", timestamp, i);
        log.info("Sending -> '{}'. ", value);
        out.println(value);
        Thread.sleep(50);
        i  = rand.nextInt(100);
      }
      log.info("Closing connection: {}. ", socket);
    }
  }

  private static void emitPurchasesWithPauses() throws Exception {
    var dataFilePath = "src/main/resources/datasets/purchases";
    log.info("Initializing socket to emit Purchases.");
    try (
        var listener = new ServerSocket(SOCKET_PORT);
        Socket socket = listener.accept()
    ) {
      log.info("Got new connection: {}. ", socket);
      BufferedReader br = new BufferedReader(new FileReader(dataFilePath));

      PrintWriter out = new PrintWriter(socket.getOutputStream(), true);
      String line;
      var count = 0;
      while ((line = br.readLine()) != null) {
        count++;
        out.println(line);
        log.info("Sending -> '{}'. ", line);
        if (count >= 10) {
          Thread.sleep(2000);
          count = 0;
        }
        else {
          Thread.sleep(50);
        }
      }
      log.info("Closing connection: {}. ", socket);
    }
  }

  private static void emitNumberPairs() throws Exception {
    log.info("Initializing socket to emit Random Numbers.");
    try (
        var listener = new ServerSocket(SOCKET_PORT);
        Socket socket = listener.accept()
    ) {
      log.info("Got new connection: {}. ", socket);
      PrintWriter out = new PrintWriter(socket.getOutputStream(), true);
      for (int i =0; i<100; i++){
        var key = (i%2)+1;
        var value = String.format("%s,%s", key, i);
        log.info("Sending -> '{}'. ", value);
        out.println(value);
        Thread.sleep(50);
      }
      log.info("Closing connection: {}. ", socket);
    }
  }

  private static void emitResignedEmployees() throws Exception {
    var dataFilePath = "src/main/resources/datasets/employees_small";
    log.info("Initializing socket to emit Employees.");
    try (
        var listener = new ServerSocket(SOCKET_PORT);
        Socket socket = listener.accept()
    ) {
      log.info("Got new connection: {}. ", socket);
      BufferedReader br = new BufferedReader(new FileReader(dataFilePath));

      PrintWriter out = new PrintWriter(socket.getOutputStream(), true);
      String line;
      while ((line = br.readLine()) != null) {
        out.println(line);
        log.info("Sending -> '{}'. ", line);
        Thread.sleep(50);
      }
      log.info("Closing connection: {}. ", socket);
    }
  }

  private static void emitTrades() throws Exception {
    var dataFilePath = "src/main/resources/datasets/trades";
    log.info("Initializing socket to emit Trades.");
    try (
        var listener = new ServerSocket(SOCKET_PORT);
        Socket socket = listener.accept()
    ) {
      log.info("Got new connection: {}. ", socket);
      BufferedReader br = new BufferedReader(new FileReader(dataFilePath));

      PrintWriter out = new PrintWriter(socket.getOutputStream(), true);
      String line;
      while ((line = br.readLine()) != null) {
        out.println(line);
        log.info("Sending -> '{}'. ", line);
        Thread.sleep(50);
      }
      log.info("Closing connection: {}. ", socket);
    }
  }
}
