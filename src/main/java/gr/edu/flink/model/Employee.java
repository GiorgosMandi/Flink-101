package gr.edu.flink.model;

public record Employee(String id,
                       String name,
                       String designation,
                       String department,
                       String departmentId
) {}
