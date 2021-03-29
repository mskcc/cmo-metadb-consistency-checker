package org.mskcc.cmo.metadb.model;

import java.util.Arrays;
import java.util.List;
import org.apache.commons.lang3.StringUtils;

/**
 *
 * @author ochoaa
 */
public class ConsistencyCheckerRequest {
    /**
     * SUCCESSFUL: Consistency checker passed and no time delays. 
     *      RequestJson is published to "cmo.new_request".
     * SUCCESSFUL_PUBLISHING_TIME_EXCEEDED: Consistency checker passed but
     *      MetaDb took longer than 300s to publish requestJson or
     *      publishing requestJson to cmo.new_request took longer than 300s. 
     *      RequestJson is published to "cmo.new_request".
     * FAILED_INCONSISTENT_REQUEST_JSONS: Inconsistent JSON format or
     *      missing attributes. RequestJson is not published to "cmo.new_request".
     * FAILED_DROPPED_MESSAGE: Received requestJson from LIMS through "igo.new_request"
     *      but never received requestJson from MetaDb through "metadb.new_request_consistency_check_topic". 
     *      RequestJson is not published to "cmo.new_request".
     * UNKNOWN_OR_INCONCLUSIVE: Inconclusive or unknown error consistency checking requestJson.
     *      RequestJson is not published to "cmo.new_request".
     */
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

    public ConsistencyCheckerRequest() {}

    public ConsistencyCheckerRequest(String date, String topic, String requestId, String incomingTimestamp, String incomingJson) {
        this.date = date;
        this.topic = topic;
        this.requestId = requestId;
        this.incomingTimestamp = incomingTimestamp;
        this.incomingJson = incomingJson;
    }

    public ConsistencyCheckerRequest(String date, String requestId, StatusType statusType, String incomingTimestamp, String outgoingTimestamp,
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
        return StringUtils.isBlank(date) ? "" : date;
    }

    public void setDate(String date) {
        this.date = date;
    }

    public String getRequestId() {
        return StringUtils.isBlank(requestId) ? "" : requestId;
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
        return StringUtils.isBlank(incomingTimestamp) ? "" : incomingTimestamp;
    }

    public void setIncomingTimestamp(String incomingTimestamp) {
        this.incomingTimestamp = incomingTimestamp;
    }

    public String getOutgoingTimestamp() {
        return StringUtils.isBlank(outgoingTimestamp) ? "" : outgoingTimestamp;
    }

    public void setOutgoingTimestamp(String outgoingTimestamp) {
        this.outgoingTimestamp = outgoingTimestamp;
    }

    public String getTopic() {
        return StringUtils.isBlank(topic) ? "" : topic;
    }

    public void setTopic(String topic) {
        this.topic = topic;
    }

    public String getIncomingJson() {
        return StringUtils.isBlank(incomingJson) ? "" : incomingJson;
    }

    public void setIncomingJson(String incomingJson) {
        this.incomingJson = incomingJson;
    }

    public String getOutgoingJson() {
        return StringUtils.isBlank(outgoingJson) ? "" : outgoingJson;
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
