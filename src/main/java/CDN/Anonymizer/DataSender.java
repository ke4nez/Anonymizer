package CDN.Anonymizer;

import CDN.HttpRecord.HttpLogDTO;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.*;
import java.util.List;
import java.util.concurrent.TimeUnit;

public class DataSender {
    private static final Logger logger = LoggerFactory.getLogger(DataSender.class);
    String url = "jdbc:clickhouse://localhost:8123";
    String proxyUrl = System.getenv("CH-PROXY");
    String user = System.getenv("CH-USERNAME");
    String password = System.getenv("PASSWORD");
    Connection connection;
    private long lastInteractionTime;

    public DataSender(){}
    public void connect() {
        try (Connection connection = DriverManager.getConnection(url, user, password)) {
            this.connection = DriverManager.getConnection(url, user, password);
            lastInteractionTime = System.currentTimeMillis();
            logger.info("CONNECTED TO CLICKHOUSE!");

           // createTable(connection);
           // checkTableExists(connection);
           // addSampleData(connection);
          //  getSampleData(connection);

             //dropTable(connection,"http_log");
           //  dropTable(connection,"http_log_agg");
            //  dropTable(connection,"http_log_mv");

          //   createTableForLogs(connection);
          //   createTableForLogsAggregation(connection);
           //  createLogMaterializedView(connection);

           getAllLogs(connection);
           getAllAggregatedLogs(connection);


        } catch (SQLException e) {
            logger.error("UNABLE TO CONNECT TO: " + url + "WITH USER: " + user);
            e.printStackTrace();
        }
    }

    public void connectToProxy() {
        try {
            String fullUrl = String.format("%s?user=%s&password=%s", proxyUrl, user, password);
            this.connection = DriverManager.getConnection(fullUrl);
            lastInteractionTime = System.currentTimeMillis();
            logger.info("CONNECTED TO CH-PROXY!");
            try {
                TimeUnit.SECONDS.sleep(62);
                getAllLogs(connection);
            } catch (InterruptedException ie) {
                Thread.currentThread().interrupt();
            }
            try {
                TimeUnit.SECONDS.sleep(62);
                getAllAggregatedLogs(connection);
            } catch (InterruptedException ie) {
                Thread.currentThread().interrupt();
            }

        } catch (SQLException e) {
            logger.error("UNABLE TO CONNECT TO CH-PROXY ON: " + url + " WITH USER: " + user);
            e.printStackTrace();
            try {
                TimeUnit.SECONDS.sleep(62);
                connectToProxy();
            } catch (InterruptedException ie) {
                Thread.currentThread().interrupt();
            }
        }
    }
    public void createTableForLogs(Connection connection) {
        String createTableSQL =
                "CREATE TABLE IF NOT EXISTS http_log ("
                + "timestamp DateTime, "
                + "resource_id UInt64, "
                + "bytes_sent UInt64, "
                + "request_time_milli UInt64, "
                + "response_status UInt16, "
                + "cache_status LowCardinality(String), "
                + "method LowCardinality(String), "
                + "remote_addr String, "
                + "url String) "
                + "ENGINE = MergeTree() "
                + "ORDER BY timestamp";

        try (Statement stmt = connection.createStatement()) {
            stmt.executeUpdate(createTableSQL);
            lastInteractionTime = System.currentTimeMillis();
            logger.debug("LOG RECORDS TABLES CREATED OR ALREADY EXISTS.");
        } catch (SQLException e) {
            logger.error("CANT CREATE TABLES FOR LOGS.");
            e.printStackTrace();
        }
    }
    public void createTableForLogsAggregation(Connection connection) {
        String createTableSQL = "CREATE TABLE IF NOT EXISTS http_log_agg ("
                        + "resource_id UInt64, "
                        + "response_status UInt16, "
                        + "cache_status LowCardinality(String), "
                        + "remote_addr String, "
                        + "total_bytes_sent UInt64, "
                        + "request_count UInt64, "
                        + "last_refresh_time DateTime DEFAULT now()) "
                        + "ENGINE = AggregatingMergeTree() "
                        + "ORDER BY (resource_id, response_status, cache_status, remote_addr)";

        try (Statement stmt = connection.createStatement()) {
            stmt.executeUpdate(createTableSQL);
            lastInteractionTime = System.currentTimeMillis();
            logger.debug("LOG RECORDS TABLES CREATED OR ALREADY EXISTS.");
        } catch (SQLException e) {
            logger.error("CANT CREATE TABLES FOR LOGS.");
            e.printStackTrace();
        }
    }
    public void createLogMaterializedView(Connection connection) {
        String createMVSQL =
                "CREATE MATERIALIZED VIEW IF NOT EXISTS http_log_mv "
                        + "TO http_log_agg "
                        + "AS SELECT "
                        + "    resource_id, "
                        + "    response_status, "
                        + "    cache_status, "
                        + "    remote_addr, "
                        + "    sum(bytes_sent) AS total_bytes_sent, "
                        + "    count() AS request_count, "
                        + "    now() AS last_refresh_time "
                        + "FROM http_log "
                        + "GROUP BY resource_id, response_status, cache_status, remote_addr";

        try (Statement stmt = connection.createStatement()) {
            stmt.executeUpdate(createMVSQL);
            logger.debug("Materialized view linked to aggregation table");
        } catch (SQLException e) {
            logger.error("Error creating MV: " + e.getMessage());
            e.printStackTrace();
        }
    }
    public List<HttpLogDTO> transferData(List<HttpLogDTO> dtoList) {
        if (dtoList.isEmpty()) {
            lastInteractionTime = System.currentTimeMillis();
            return dtoList;
        }

        String insertSQL = "INSERT INTO http_log (timestamp, resource_id, bytes_sent, request_time_milli, "
                + "response_status, cache_status, method, remote_addr, url) "
                + "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)";

        try (PreparedStatement stmt = connection.prepareStatement(insertSQL)) {
            for (HttpLogDTO dto : dtoList) {
                long timestampSeconds = dto.getTimestampEpochMilli() / 1000L;
                stmt.setInt(1, (int) timestampSeconds);
                stmt.setLong(2, dto.getResourceId());
                stmt.setLong(3, dto.getBytesSent());
                stmt.setLong(4, dto.getRequestTimeMilli());
                stmt.setShort(5, dto.getResponseStatus());
                stmt.setString(6, dto.getCacheStatus());
                stmt.setString(7, dto.getMethod());
                stmt.setString(8, dto.getRemoteAddr());
                stmt.setString(9, dto.getUrl());
                stmt.addBatch();
            }

            int[] result = stmt.executeBatch();
            lastInteractionTime = System.currentTimeMillis();
            boolean isCorrect = true;
            for(int qresult : result){
                if (qresult < 0) {
                    isCorrect = false;
                    break;
                }
            }
            if(isCorrect) {
                dtoList.clear();
                return dtoList;
            }
            else{
                return  dtoList;
            }

        } catch (SQLException e) {
            logger.error("Batch insert failed: " + e.getMessage());
            e.printStackTrace();
            return dtoList;
        }
    }
    public void dropTable(Connection connection, String tableName) {
        String dropTableSQL = "DROP TABLE IF EXISTS " + tableName;

        try (Statement stmt = connection.createStatement()) {
            stmt.executeUpdate(dropTableSQL);
            lastInteractionTime = System.currentTimeMillis();
            logger.debug("TABLE '" + tableName + "' DROPPED OR DOS NOT EXISTS.");
        } catch (SQLException e) {
            logger.error("ERROR ON DROPPING TABLE WITH NAME:  '" + tableName + "'");
            e.printStackTrace();
        }
    }
    public void getAllLogs(Connection connection) {
        String selectSQL = "SELECT * FROM http_log";
        lastInteractionTime = System.currentTimeMillis();
        try (Statement stmt = connection.createStatement();
             ResultSet rs = stmt.executeQuery(selectSQL)) {

            System.out.println("\nhttp_log:");
            System.out.println("----------------------------------------------------------------------------------------------------------------------------------");
            System.out.printf("| %-30s | %-12s | %-10s | %-16s | %-6s | %-12s | %-7s | %-15s | %-50s |%n",
                    "Timestamp", "Resource ID", "Bytes Sent", "Request Time", "Status", "Cache Status", "Method", "Remote Addr", "URL");
            System.out.println("----------------------------------------------------------------------------------------------------------------------------------");

            while (rs.next()) {
                java.sql.Timestamp timestamp = rs.getTimestamp("timestamp");
                long resourceId = rs.getLong("resource_id");
                long bytesSent = rs.getLong("bytes_sent");
                long requestTime = rs.getLong("request_time_milli");
                int responseStatus = rs.getInt("response_status");
                String cacheStatus = rs.getString("cache_status");
                String method = rs.getString("method");
                String remoteAddr = rs.getString("remote_addr");
                String url = rs.getString("url");

                System.out.printf("| %-30s | %-12d | %-10d | %-16d | %-6d | %-12s | %-7s | %-15s | %-50s |%n",
                        timestamp,
                        resourceId,
                        bytesSent,
                        requestTime,
                        responseStatus,
                        cacheStatus,
                        method,
                        remoteAddr,
                        url.length() > 50 ? url.substring(0, 47) + "..." : url);
            }

            System.out.println("----------------------------------------------------------------------------------------------------------------------------------");

        } catch (SQLException e) {
            logger.error("CANT LOAD LOGS FROM DATABASE: " + e.getMessage());
            e.printStackTrace();
        }
    }
    public void getAllAggregatedLogs(Connection connection) {
        String selectSQL = "SELECT * FROM http_log_agg";

        try (Statement stmt = connection.createStatement();
             ResultSet rs = stmt.executeQuery(selectSQL)) {

            System.out.println("\nhttp_log_agg:");
            System.out.println("----------------------------------------------------------------------------------------------------------------------");
            System.out.printf("| %-12s | %-6s | %-12s | %-15s | %-15s | %-13s | %-30s |%n",
                    "Resource ID", "Status", "Cache Status", "Remote Addr", "Total Bytes", "Request Count", "Last Refresh");
            System.out.println("----------------------------------------------------------------------------------------------------------------------");

            while (rs.next()) {
                long resourceId = rs.getLong("resource_id");
                int responseStatus = rs.getInt("response_status");
                String cacheStatus = rs.getString("cache_status");
                String remoteAddr = rs.getString("remote_addr");
                long totalBytes = rs.getLong("total_bytes_sent");
                long requestCount = rs.getLong("request_count");
                Timestamp lastRefresh = rs.getTimestamp("last_refresh_time");

                System.out.printf("| %-12d | %-6d | %-12s | %-15s | %-15d | %-13d | %-30s |%n",
                        resourceId,
                        responseStatus,
                        cacheStatus,
                        remoteAddr,
                        totalBytes,
                        requestCount,
                        lastRefresh);
            }

            System.out.println("----------------------------------------------------------------------------------------------------------------------");

        } catch (SQLException e) {
            logger.error("Ошибка загрузки агрегированных данных: " + e.getMessage());
            e.printStackTrace();
        }
    }

    public long getDuration() {
        long diffMillis = System.currentTimeMillis() - lastInteractionTime;
        return diffMillis / 1000;
    }
}
