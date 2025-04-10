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
    private long lastProxyInteractionTime;
    private long lastAggLogsUpdateTime = System.currentTimeMillis();

    public DataSender(){}
    public void connect() {
        try {
            this.connection = DriverManager.getConnection(url, user, password);
            lastProxyInteractionTime = System.currentTimeMillis();
            logger.info("CONNECTED TO CLICKHOUSE!");

        } catch (SQLException e) {
            logger.error("Unable to connect to: " + url + "with user: " + user + e.getMessage());
        }
    }

    public void connectToProxy() {
        try {
            String fullUrl = String.format("%s?user=%s&password=%s", proxyUrl, user, password);
            this.connection = DriverManager.getConnection(fullUrl);
            lastProxyInteractionTime = System.currentTimeMillis();
            logger.info("CONNECTED TO CH-PROXY!");

        } catch (SQLException e) {
            logger.error("Unable to connect to ch-proxy on: " + url + " with user: " + user + e.getMessage());
            try {
                TimeUnit.SECONDS.sleep(62);
                connectToProxy();
            } catch (InterruptedException ie) {
                Thread.currentThread().interrupt();
            }
        }
    }
    public void createTableForLogs() {
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
            lastProxyInteractionTime = System.currentTimeMillis();
            logger.debug("Log records tables created or already exists.");
        } catch (SQLException e) {
            logger.error("Cant create tables for logs." + e.getMessage());
        }
    }
    public void createTableForLogsAggregation(Connection connection) {
        String createTableSQL = "CREATE TABLE IF NOT EXISTS http_log_agg ("
                + "resource_id UInt64, "
                + "response_status UInt16, "
                + "cache_status LowCardinality(String), "
                + "remote_addr String, "
                + "total_bytes_sent UInt64, "
                + "request_count UInt64) "
                + "ENGINE = ReplacingMergeTree(total_bytes_sent) "
                + "ORDER BY (resource_id, response_status, cache_status, remote_addr)";

        try (Statement stmt = connection.createStatement()) {
            stmt.executeUpdate(createTableSQL);
            lastProxyInteractionTime = System.currentTimeMillis();
            logger.debug("Log records aggregation table created or already exists.");
        } catch (SQLException e) {
            logger.error("Cannot create aggregation table for logs. " + e.getMessage());
        }
    }
    public void refreshAggregatedTable() {
        String insertDataSQL =
                "INSERT INTO http_log_agg "
                        + "SELECT "
                        + "  resource_id, "
                        + "  response_status, "
                        + "  cache_status, "
                        + "  remote_addr, "
                        + "  sum(bytes_sent) AS total_bytes_sent, "
                        + "  count() AS request_count "
                        + "FROM http_log "
                        + "GROUP BY resource_id, response_status, cache_status, remote_addr";

        try (Statement stmt = connection.createStatement()) {
            stmt.executeUpdate(insertDataSQL);
            logger.debug("Aggregated data inserted into 'http_log_agg' successfully.");

            lastAggLogsUpdateTime = System.currentTimeMillis();
        } catch (SQLException e) {
            logger.error("Failed to refresh aggregation table: " + e.getMessage());
        }
    }
    public List<HttpLogDTO> transferData(List<HttpLogDTO> dtoList) {
        logger.info("adding dto list with: " + dtoList.size() + " records");
        if (dtoList.isEmpty()) {
            lastProxyInteractionTime = System.currentTimeMillis();
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
            lastProxyInteractionTime = System.currentTimeMillis();
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
            return dtoList;
        }
    }
    public void dropTable(String tableName) {
        String dropTableSQL = "DROP TABLE IF EXISTS " + tableName;

        try (Statement stmt = connection.createStatement()) {
            stmt.executeUpdate(dropTableSQL);
            lastProxyInteractionTime = System.currentTimeMillis();
            logger.debug("Table '" + tableName + "' dropped or dos not exists.");
        } catch (SQLException e) {
            logger.error("error on dropping table with name:  '" + tableName + "'" + e.getMessage());
        }
    }
    public void getAllLogs() {
        String selectSQL = "SELECT * FROM http_log";
        int counter = 0;
        lastProxyInteractionTime = System.currentTimeMillis();
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
                counter++;
            }

            System.out.println("----------------------------------------------------------------------------------------------------------------------------------");
            System.out.println("Rows in log table: " + counter);
        } catch (SQLException e) {
            logger.error("Cant load logs from database: " + e.getMessage());
        }
    }
    public void getAllAggregatedLogs() {
        String selectSQL = "SELECT resource_id, response_status, cache_status, remote_addr, "
                + "total_bytes_sent, request_count "
                + "FROM http_log_agg "
                + "ORDER BY resource_id, response_status, cache_status, remote_addr";

        int counter = 0;

        try (Statement stmt = connection.createStatement();
             ResultSet rs = stmt.executeQuery(selectSQL)) {

            System.out.println("\nhttp_log_agg:");
            System.out.println("----------------------------------------------------------------------------------------------------------------------");
            System.out.printf("| %-12s | %-6s | %-12s | %-15s | %-15s | %-13s |%n",
                    "Resource ID", "Status", "Cache Status", "Remote Addr", "Total Bytes", "Request Count");
            System.out.println("----------------------------------------------------------------------------------------------------------------------");

            while (rs.next()) {
                long resourceId = rs.getLong("resource_id");
                int responseStatus = rs.getInt("response_status");
                String cacheStatus = rs.getString("cache_status");
                String remoteAddr = rs.getString("remote_addr");
                long totalBytes = rs.getLong("total_bytes_sent");
                long requestCount = rs.getLong("request_count");

                System.out.printf("| %-12d | %-6d | %-12s | %-15s | %-15d | %-13d |%n",
                        resourceId,
                        responseStatus,
                        cacheStatus,
                        remoteAddr,
                        totalBytes,
                        requestCount);
                counter++;
            }

            System.out.println("----------------------------------------------------------------------------------------------------------------------");
            System.out.println("Rows in agg table: " + counter);
        } catch (SQLException e) {
            logger.error("Error loading aggregated data: " + e.getMessage());
        }
    }

    public void optimizeTable() {
        String dropTableSQL = "OPTIMIZE TABLE http_log_agg FINAL";

        try (Statement stmt = connection.createStatement()) {
            stmt.executeUpdate(dropTableSQL);
            lastProxyInteractionTime = System.currentTimeMillis();
        } catch (SQLException e) {
            logger.error(e.getMessage());
        }
    }

    public long getProxyDuration() {
        long diffMillis = System.currentTimeMillis() - lastProxyInteractionTime;
        return diffMillis / 1000;
    }

    public long getOptimizeDuration(){
         long diffMillis = System.currentTimeMillis() - lastAggLogsUpdateTime;
         return diffMillis / 1000;
    }
}
