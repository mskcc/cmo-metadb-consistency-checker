package org.mskcc.cmo.metadb.util;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.base.Strings;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.skyscreamer.jsonassert.JSONAssert;
import org.skyscreamer.jsonassert.JSONCompareMode;
import org.springframework.stereotype.Component;

@Component
public class ConsistencyCheckerUtil {

    private final ObjectMapper mapper = new ObjectMapper();
    private final String[] DEFAULT_IGNORED_FIELDS = new String[]{
        "metaDbRequestId",
        "metaDbSampleId",
        "metaDbPatientId",
        "requestJson",
        "samples"};

    private final String CONSISTENCY_CHECKER_FAILURES_FILE_HEADER =
            "DATE\tREASON\tREFERENCEJSON\tTARGETJSON\n";
    private final String MISSING_OR_NULL_JSON = "JSON string is NULL";
    private final String SUCCESSFUL_CONSISTENCY_CHECK = "Both JSON strings are equal";
    private final String FAILED_CONSISTENCY_CHECK = "JSON strings are NOT equal";

    /**
     *
     * @param referenceJson
     * @param targetJson
     * @param ignoredFields
     * @return boolean
     * @throws Exception
     */
    public Boolean isConsistent(String referenceJson, String targetJson,
            String[] ignoredFields) throws Exception {
        //File loggerFile = fileUtil.getOrCreateFileWithHeader(metadbCheckerFailuresFilepath,
        //CONSISTENCY_CHECKER_FAILURES_FILE_HEADER);
        if (referenceJson == null ? targetJson == null : referenceJson.equals(targetJson)) {
            return true;
        }
        // get map of filtered json strings for reference json and target json
        Map<String, String> referenceFilteredJsonMap = getRequestJsonNodesMap(referenceJson, ignoredFields);
        Map<String, String> targetFilteredJsonMap = getRequestJsonNodesMap(targetJson, ignoredFields);

        boolean assertResponse = false;
        try {
            JSONAssert.assertEquals(referenceFilteredJsonMap.get("requestNode"),
                    targetFilteredJsonMap.get("requestNode"), JSONCompareMode.STRICT);
            JSONAssert.assertEquals(referenceFilteredJsonMap.get("samplesNode"),
                    targetFilteredJsonMap.get("samplesNode"), JSONCompareMode.STRICT);

            assertResponse = true;
            //fileUtil.writeToFile(loggerFile, SUCCESSFUL_CONSISTENCY_CHECK);
        } catch (Exception e) {
            //fileUtil.writeToFile(loggerFile, FAILED_CONSISTENCY_CHECK);
            e.printStackTrace();
        }
        return assertResponse;
    }

    public Boolean isConsistent(String referenceJson, String targetJson) throws Exception {
        return isConsistent(referenceJson, targetJson, DEFAULT_IGNORED_FIELDS);
    }

    /**
     * TODO?
     * @param referenceJson
     * @param targetJson
     * @return
     */
    public Object getJsonDiffs(String referenceJson, String targetJson) {
        throw new UnsupportedOperationException("Method not supported yet");
    }

    /**
     * Returns an ordered ArrayNode of the 'samples' JSON attribute from the request json.
     * @param samples
     * @param ignoredFields
     * @return ArrayNode
     * @throws JsonProcessingException
     */
    private ArrayNode getOrderedRequestSamplesJson(ArrayNode samples,
            String[] ignoredFields) throws JsonProcessingException {
        Map<String, JsonNode> unorderedSamplesMap = new HashMap<>();
        Iterator<JsonNode> itr = samples.elements();
        while (itr.hasNext()) {
            JsonNode n = filterJsonNode((ObjectNode) itr.next(), ignoredFields);
            String sid = n.get("igoId").toString();
            unorderedSamplesMap.put(sid, n);
        }

        LinkedHashMap<String, JsonNode> orderedSamplesMap = new LinkedHashMap<>();
        unorderedSamplesMap.entrySet()
            .stream()
            .sorted(Map.Entry.comparingByKey())
            .forEachOrdered(x -> orderedSamplesMap.put(x.getKey(), x.getValue()));

        ObjectMapper mapper = new ObjectMapper();
        ArrayNode sortedJsonNodeSamples = mapper.createArrayNode();
        orderedSamplesMap.entrySet().forEach((entry) -> {
            sortedJsonNodeSamples.add(entry.getValue());
        });
        return sortedJsonNodeSamples;
    }

    /**
     *
     * @param reason
     * @param referenceJson
     * @param targetJson
     * @return string
     */
    public String failureMessageAppender(String reason, String referenceJson, String targetJson) {
        String currentDate = LocalDate.now().format(DateTimeFormatter.ISO_LOCAL_DATE);
        StringBuilder builder = new StringBuilder();
        builder.append(currentDate)
                .append("\t")
                .append(reason)
                .append("\t")
                .append(referenceJson)
                .append("\t")
                .append(targetJson)
                .append("\n");
        return builder.toString();
    }

    /**
     * Returns map containing the filtered request json node and samples node.
     * @param jsonString
     * @return Map
     * @throws JsonProcessingException
     */
    private Map<String, String> getRequestJsonNodesMap(String jsonString,
            String[] ignoredFields) throws JsonProcessingException {
        JsonNode unfilteredJsonNode = mapper.readTree(jsonString);
        ArrayNode samplesJsonNode = null;
        if (unfilteredJsonNode.has("samples")) {
            samplesJsonNode = getOrderedRequestSamplesJson(
                    (ArrayNode) unfilteredJsonNode.get("samples"), ignoredFields);
        }

        JsonNode filteredNode = filterJsonNode((ObjectNode) unfilteredJsonNode, ignoredFields);
        Map<String, String> filteredJsonNodesMap = new HashMap<>();
        filteredJsonNodesMap.put("requestNode", mapper.writeValueAsString(filteredNode));
        filteredJsonNodesMap.put("samplesNode", mapper.writeValueAsString(samplesJsonNode));
        return filteredJsonNodesMap;
    }

    /**
     * Returns a filtered JsonNode instance.
     * @param node
     * @return JsonNode
     */
    private JsonNode filterJsonNode(ObjectNode node, String[] ignoredFields) {
        for (String field : ignoredFields) {
            if (node.has(field)) {
                node.remove(field);
            }
        }

        Iterator<String> itr = node.fieldNames();
        List<String> removeFieldNames = new ArrayList<>();
        while (itr.hasNext()) {
            String fieldName = itr.next();
            JsonNode value = node.get(fieldName);
            if (Strings.isNullOrEmpty(value.asText()) || value.asText().equals("null")) {
                removeFieldNames.add(fieldName);
            }
        }
        for (String field : removeFieldNames) {
            node.remove(field);
        }
        return node;
    }
}
