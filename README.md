# Flink 101 Tutorial

Tutorial based on this [course](https://www.udemy.com/course/apache-flink-a-real-time-hands-on-course-on-flink/?couponCode=ST21MT30625G2)

To run, start the cluster using

```bash
$ path/to/flink/bin/start-cluster.sh
```

and create fat-jar

```bash
mvn clean package
```

then execute using

```bash
$ path/to/flink/bin/flink run -c gr.edu.flink.<class> ./target/original-Flink-101-1.0-SNAPSHOT.jar
```