package org.mskcc.cmo.metadb.logger;

import java.util.Arrays;
import java.util.List;
import org.apache.commons.lang3.StringUtils;

/**
 *
 * @author ochoaa
 */
public class ConsistencyCheckerLogger {
    public enum StatusType {
        SUCCESSFUL,
        SUCCESSFUL_PUBLISHING_TIME_EXCEEDED,
        FAILED_INCONSISTENT_REQUEST_JSONS,
        FAILED_DROPPED_MESSAGE,
        UNKNOWN_OR_INCONCLUSIVE
    }

    private String date;
    private String requestId;
    private StatusType statusType;
    private String incomingTimestamp;
    private String outgoingTimestamp;
    private String topic;
    private String incomingJson;
    private String outgoingJson;

    public ConsistencyCheckerLogger() {}

    public ConsistencyCheckerLogger(String date, String topic, String requestId, String incomingTimestamp, String incomingJson) {
        this.date = date;
        this.topic = topic;
        this.requestId = requestId;
        this.incomingTimestamp = incomingTimestamp;
        this.incomingJson = incomingJson;
    }

    public ConsistencyCheckerLogger(String date, String requestId, StatusType statusType, String incomingTimestamp, String outgoingTimestamp,
            String topic, String incomingJson, String outgoingJson) {
        this.date = date;
        this.requestId = requestId;
        this.statusType = statusType;
        this.incomingTimestamp = incomingTimestamp;
        this.outgoingTimestamp = outgoingTimestamp;
        this.topic = topic;
        this.incomingJson = incomingJson;
        this.outgoingJson = outgoingJson;
    }

    public String getDate() {
        return date == null ? "" : date;
    }

    public void setDate(String date) {
        this.date = date;
    }

    public String getRequestId() {
        return requestId == null ? "" : requestId;
    }

    public void setRequestId(String requestId) {
        this.requestId = requestId;
    }

    public StatusType getStatusType() {
        return statusType == null ? StatusType.UNKNOWN_OR_INCONCLUSIVE : statusType;
    }

    public void setStatusType(StatusType statusType) {
        this.statusType = statusType;
    }

    public String getIncomingTimestamp() {
        return incomingTimestamp == null ? "" : incomingTimestamp;
    }

    public void setIncomingTimestamp(String incomingTimestamp) {
        this.incomingTimestamp = incomingTimestamp;
    }

    public String getOutgoingTimestamp() {
        return outgoingTimestamp == null ? "" : outgoingTimestamp;
    }

    public void setOutgoingTimestamp(String outgoingTimestamp) {
        this.outgoingTimestamp = outgoingTimestamp;
    }

    public String getTopic() {
        return topic == null ? "" : topic;
    }

    public void setTopic(String topic) {
        this.topic = topic;
    }

    public String getIncomingJson() {
        return incomingJson == null ? "" : incomingJson;
    }

    public void setIncomingJson(String incomingJson) {
        this.incomingJson = incomingJson;
    }

    public String getOutgoingJson() {
        return outgoingJson == null ? "" : outgoingJson;
    }

    public void setOutgoingJson(String outgoingJson) {
        this.outgoingJson = outgoingJson;
    }

    public String getConsistencyCheckerFileHeader() {
        return StringUtils.join(getConsistencyCheckerLoggerFields(), "\t");
    }

    public List<String> getConsistencyCheckerLoggerFields() {
        return Arrays.asList("DATE", "REQUEST_ID", "STATUS", "INCOMING_TIMESTAMP",
                "OUTGOING_TIMESTAMP", "TOPIC", "INCOMING_JSON", "TARGET_JSON");
    }

    public List<String> getConsistencyCheckerLoggerRecord() {
        return Arrays.asList(getDate(), getRequestId(), getStatusType().toString(),
                getIncomingTimestamp(), getOutgoingTimestamp(),
                getIncomingJson(), getOutgoingJson());
    }

    @Override
    public String toString() {
        return StringUtils.join(getConsistencyCheckerLoggerRecord(), "\t");
    }
}
