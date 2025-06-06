package CDN.HttpRecord;

public class HttpLogDTO {
    private final long timestampEpochMilli;
    private final long resourceId;
    private final long bytesSent;
    private final long requestTimeMilli;
    private final short responseStatus;
    private final String cacheStatus;
    private final String method;
    private final String remoteAddr;
    private final String url;

    public HttpLogDTO(long timestampEpochMilli, long resourceId, long bytesSent, long requestTimeMilli,
                      short responseStatus, String cacheStatus, String method, String remoteAddr, String url) {
        this.timestampEpochMilli = timestampEpochMilli;
        this.resourceId = resourceId;
        this.bytesSent = bytesSent;
        this.requestTimeMilli = requestTimeMilli;
        this.responseStatus = responseStatus;
        this.cacheStatus = cacheStatus;
        this.method = method;
        this.remoteAddr = anonymize(remoteAddr);
        this.url = url;
    }

    private String anonymize(String remoteAddr) {
        if (remoteAddr == null || remoteAddr.isEmpty()) {
            return null;
        }

        String[] parts = remoteAddr.split("\\.");
        if (parts.length == 4) {
            parts[3] = "X";
            return String.join(".", parts);
        }

        return null;
    }

    public long getTimestampEpochMilli() {
        return timestampEpochMilli;
    }

    public long getResourceId() {
        return resourceId;
    }

    public long getBytesSent() {
        return bytesSent;
    }

    public long getRequestTimeMilli() {
        return requestTimeMilli;
    }

    public short getResponseStatus() {
        return responseStatus;
    }

    public String getCacheStatus() {
        return cacheStatus;
    }

    public String getMethod() {
        return method;
    }

    public String getRemoteAddr() {
        return remoteAddr;
    }

    public String getUrl() {
        return url;
    }
}