package org.mskcc.cmo.metadb.util;
import java.io.IOException;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;
import org.mskcc.cmo.common.FileUtil;
import org.skyscreamer.jsonassert.Customization;
import org.skyscreamer.jsonassert.JSONAssert;
import org.skyscreamer.jsonassert.JSONCompareMode;
import org.skyscreamer.jsonassert.comparator.CustomComparator;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;

@Component
public class ConsistencyCheckerUtil {
    
    @Autowired
    private FileUtil fileUtil;
    
    @Value("${metadb.consistency_checker_failures_filepath}")
    private String metadbCheckerFailuresFilepath;
    
    private final String CONSISTENCY_CHECKER_FAILURES_FILE_HEADER = "DATE\tREASON\tREFERENCEJSON\tTARGETJSON\n";
    private final String MISSING_OR_NULL_JSON = "JSON string is NULL";
    private final String SUCCESSFUL_CONSISTENCY_CHECK = "Both JSON strings are equal";
    private final String FAILED_CONSISTENCY_CHECK = "JSON strings are NOT equal";
   
    public boolean isConsistent(String referenceJson, String targetJson, List<String> ignoredFields) throws IOException {
        //File loggerFile = fileUtil.getOrCreateFileWithHeader(metadbCheckerFailuresFilepath, CONSISTENCY_CHECKER_FAILURES_FILE_HEADER);
        if (referenceJson == targetJson) {
            //fileUtil.writeToFile(loggerFile, SUCCESSFUL_CONSISTENCY_CHECK);
            return true;
        }
        ObjectMapper mapper = new ObjectMapper();
        
        JsonNode referenceNode = mapper.readTree(referenceJson);
        JsonNode targetNode = mapper.readTree(targetJson);
        
        if (referenceNode.isNull() || targetNode.isNull()) {
            //fileUtil.writeToFile(loggerFile, MISSING_OR_NULL_JSON);
            return false;
        }

        ObjectNode referenceNodeNoSamples = (ObjectNode) referenceNode.deepCopy();
        JsonNode referenceSamples = referenceNodeNoSamples.remove("samples");
        
        ObjectNode targetNodeNoSamples = (ObjectNode) targetNode.deepCopy();
        JsonNode targetSamples = targetNodeNoSamples.remove("samples");
        targetNodeNoSamples.remove("metaDbRequestId");
        targetNodeNoSamples.remove("requestJson");
        
        boolean assertResponse = false;
        try {
            JSONAssert.assertEquals(mapper.writeValueAsString(referenceNodeNoSamples), 
                    mapper.writeValueAsString(targetNodeNoSamples), 
                    new CustomComparator (JSONCompareMode.STRICT,
                            new Customization("metaDbRequestId", (o1, o2) -> true),
                            new Customization("requestJson", (o1, o2) -> true)));

//            JSONAssert.assertEquals(mapper.writeValueAsString(referenceSamples), 
//                    mapper.writeValueAsString(targetSamples), 
//                    new CustomComparator (JSONCompareMode.STRICT,
//                            new Customization("metaDbSampleId", (o1, o2) -> true), 
//                            new Customization("metaDbPatientId", (o1, o2) -> true)));
            assertResponse = true;
            //fileUtil.writeToFile(loggerFile, SUCCESSFUL_CONSISTENCY_CHECK);
        } catch(Exception e) {
            //fileUtil.writeToFile(loggerFile, FAILED_CONSISTENCY_CHECK);
            e.printStackTrace();
        }       
        return assertResponse;
    }
    
    public boolean isConsistent(String referenceJson, String targetJson) throws IOException {
        List<String> ignoredFields = new ArrayList<>();
        ignoredFields.add("metaDbRequestId");
        ignoredFields.add("metaDbSampleId");
        ignoredFields.add("metaDbPatientId");
        return isConsistent(referenceJson, targetJson, ignoredFields);
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
}