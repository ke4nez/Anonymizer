package CDN.Anonymizer;

import CDN.HttpRecord.HttpLogDTO;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.*;
import java.util.List;
import java.util.concurrent.TimeUnit;

public class DataSender {
    private static final Logger logger = LoggerFactory.getLogger(DataSender.class);
    String proxyUrl = System.getenv("CH-PROXY");
    String user = System.getenv("CH-USERNAME");
    String password = System.getenv("PASSWORD");
    Connection connection;
    private long lastProxyInteractionTime;
    private long lastAggLogsUpdateTime = System.currentTimeMillis();

    public DataSender(){}

    public void connectToProxy() {
        try {
            String fullUrl = String.format("%s?user=%s&password=%s", proxyUrl, user, password);
            this.connection = DriverManager.getConnection(fullUrl);
            lastProxyInteractionTime = System.currentTimeMillis();
            logger.info("CONNECTED TO CH-PROXY!");

        } catch (SQLException e) {
            logger.error("Unable to connect to ch-proxy on: " + proxyUrl + " with user: " + user + e.getMessage());
            try {
                TimeUnit.SECONDS.sleep(62);
                connectToProxy();
            } catch (InterruptedException ie) {
                Thread.currentThread().interrupt();
            }
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
        logger.info("Adding dto list with: " + dtoList.size() + " records");
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

    public long getProxyDuration() {
        long diffMillis = System.currentTimeMillis() - lastProxyInteractionTime;
        return diffMillis / 1000;
    }

    public long getOptimizeDuration(){
         long diffMillis = System.currentTimeMillis() - lastAggLogsUpdateTime;
         return diffMillis / 1000;
    }
}
