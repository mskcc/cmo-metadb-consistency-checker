package org.mskcc.cmo.metadb.util;
import com.fasterxml.jackson.core.JsonProcessingException;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import org.mskcc.cmo.common.FileUtil;
import org.skyscreamer.jsonassert.JSONAssert;
import org.skyscreamer.jsonassert.JSONCompareMode;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;

@Component
public class ConsistencyCheckerUtil {

    @Autowired
    private FileUtil fileUtil;

    @Value("${metadb.consistency_checker_failures_filepath}")
    private String metadbCheckerFailuresFilepath;

    private final ObjectMapper mapper = new ObjectMapper();
    private final String[] DEFAULT_IGNORED_FIELDS = new String[]{"metaDbRequestId", "metaDbSampleId", "metaDbPatientId", "requestJson", "samples"};

    private final String CONSISTENCY_CHECKER_FAILURES_FILE_HEADER = "DATE\tREASON\tREFERENCEJSON\tTARGETJSON\n";
    private final String MISSING_OR_NULL_JSON = "JSON string is NULL";
    private final String SUCCESSFUL_CONSISTENCY_CHECK = "Both JSON strings are equal";
    private final String FAILED_CONSISTENCY_CHECK = "JSON strings are NOT equal";

    public boolean isConsistent(String referenceJson, String targetJson, String[] ignoredFields) throws Exception {
        //File loggerFile = fileUtil.getOrCreateFileWithHeader(metadbCheckerFailuresFilepath, CONSISTENCY_CHECKER_FAILURES_FILE_HEADER);
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
        } catch(Exception e) {
            //fileUtil.writeToFile(loggerFile, FAILED_CONSISTENCY_CHECK);
            e.printStackTrace();
        }
        return assertResponse;
    }

    /**
     * Returns an ordered ArrayNode of the 'samples' JSON attribute from the request json.
     * @param samples
     * @param ignoredFields
     * @return ArrayNode
     * @throws JsonProcessingException
     */
    private ArrayNode getOrderedRequestSamplesJson(ArrayNode samples, String[] ignoredFields) throws JsonProcessingException {
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

    public boolean isConsistent(String referenceJson, String targetJson) throws Exception {
        return isConsistent(referenceJson, targetJson, DEFAULT_IGNORED_FIELDS);
    }

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
    private Map<String, String> getRequestJsonNodesMap(String jsonString, String[] ignoredFields) throws JsonProcessingException {
        JsonNode unfilteredJsonNode = mapper.readTree(jsonString);
        ArrayNode samplesJsonNode = null;
        if (unfilteredJsonNode.has("samples")) {
            samplesJsonNode = getOrderedRequestSamplesJson((ArrayNode) unfilteredJsonNode.get("samples"), ignoredFields);
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
        // check value of node, if empty or null then remove also
        // will need to iterate through the node itself after
        // going through and removing the ignored fields above
        // see example iterator set up in getOrderedRequestSamplesJson()
        return node;
    }

}
