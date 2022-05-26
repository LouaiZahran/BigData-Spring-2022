package com.university.bigdata.milestone3;

import java.sql.*;

public class Query {
    public static Info3[] getQuery(long start, long end) {
        Info3[] batch = batchQuery(start, end);
        Info3[] realtime = realtimeQuery(start, end);
        for (int i = 0; i < 5; i++) {
            batch[i].update(realtime[i]);
        }
        return batch;
    }

    public static void main(String[] args) {
        long start = 0, end = 20;
        Info3[] out = getQuery(start, end);
        for (int i = 1; i < 5; i++) {
            System.out.print(out[i].CPU/out[i].count + " ");
            System.out.print(out[i].RAM/out[i].count  + " ");
            System.out.print(out[i].Disk /out[i].count + " ");
            System.out.print(out[i].maxCPUtime + " ");
            System.out.print(out[i].maxRAMtime + " ");
            System.out.print(out[i].maxDISKtime + " ");
            System.out.println(out[i].count + " ");
        }
    }

    private static Info3[] batchQuery(long start, long end) {
        Info3[] out = new Info3[5];
        for (int i = 0; i < 5; i++) {
            out[i] = new Info3(i);
        }
        try {
            Class.forName("org.duckdb.DuckDBDriver");
            Connection conn = DriverManager.getConnection("jdbc:duckdb:");
            Statement stmt = conn.createStatement();
            ResultSet rs = stmt.executeQuery(
                    "SELECT * FROM read_parquet('batch_view/*.parquet') WHERE time BETWEEN " + start + " AND " + end
                            + ";");
            while (rs.next()) {
                int i = rs.getInt("service");
                Info3 other = new Info3(i,
                        rs.getDouble("CPU"),
                        rs.getDouble("RAM"),
                        rs.getDouble("DISK"),
                        rs.getInt("count"),
                        rs.getLong("time"),
                        rs.getLong("time"),
                        rs.getLong("time"),
                        rs.getDouble("CPU_MAX"),
                        rs.getDouble("RAM_MAX"),
                        rs.getDouble("DISK_MAX"));

                out[i].update(other);
            }
            rs.close();
        } catch (Exception e) {
            System.out.println("noooo");
        }
        return out;
    }

    private static Info3[] realtimeQuery(long start, long end) {
        Info3[] out = new Info3[5];
        for (int i = 0; i < 5; i++) {
            out[i] = new Info3(i);
        }
        try {
            Class.forName("org.duckdb.DuckDBDriver");
            Connection conn = DriverManager.getConnection("jdbc:duckdb:");
            Statement stmt = conn.createStatement();
            ResultSet rs = stmt.executeQuery(
                    "SELECT * FROM read_parquet('realtime_view/*.parquet') WHERE time BETWEEN " + start + " AND " + end
                            + ";");
            while (rs.next()) {
                int i = rs.getInt("service") ;
                Info3 other = new Info3(i,
                        rs.getDouble("CPU"),
                        rs.getDouble("RAM"),
                        rs.getDouble("DISK"),
                        rs.getInt("count"),
                        rs.getLong("time"),
                        rs.getLong("time"),
                        rs.getLong("time"),
                        rs.getDouble("CPU_MAX"),
                        rs.getDouble("RAM_MAX"),
                        rs.getDouble("DISK_MAX"));
                out[i].update(other);
            }
            rs.close();
        } catch (Exception e) {
            System.out.println("noooo");
        }
        return out;
    }

}
