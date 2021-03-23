package org.mskcc.cmo.metadb;

import java.util.*;
import junit.framework.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mskcc.cmo.metadb.config.MockDataConfig;
import org.mskcc.cmo.metadb.model.MockJsonTestData;
import org.mskcc.cmo.metadb.util.ConsistencyCheckerUtil;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

/**
 *
 * @author ochoaa
 */
@ContextConfiguration(classes = MockDataConfig.class)
@RunWith(SpringJUnit4ClassRunner.class)
public class ConsistencyCheckerUtilTest {
    @Autowired
    private ConsistencyCheckerUtil consistencyCheckerUtil;

    private Map<String, String> requestJsonDataIdMap;
    @Autowired
    private void initRequestJsonDataIdMap() {
        this.requestJsonDataIdMap = new HashMap<>();
        requestJsonDataIdMap.put("mockIncomingRequest1JsonDataWith2T2N",
                "mockPublishedRequest1JsonDataWith2T2N");
        requestJsonDataIdMap.put("mockIncomingRequest2aJsonData1N",
                "mockPublishedRequest2aJsonData1N");
        requestJsonDataIdMap.put("mockIncomingRequest2bJsonDataMissing1N",
                "mockPublishedRequest2bJsonDataComplete");
        requestJsonDataIdMap.put("mockIncomingRequest3JsonDataPooledNormals",
                "mockPublishedRequest3JsonDataPooledNormals");
    }

    @Autowired
    private Map<String, MockJsonTestData> mockedRequestJsonDataMap;

    /**
     * Tests to ensure the mocked request json data map is not null and
     * contains all id's from 'requestJsonDataIdMap'.
     */
    @Test
    public void testMockedRequestJsonDataLoading() {
        Assert.assertNotNull(mockedRequestJsonDataMap);

        for (Map.Entry<String, String> entry : requestJsonDataIdMap.entrySet()) {
            Assert.assertTrue(mockedRequestJsonDataMap.containsKey(entry.getKey()));
            Assert.assertTrue(mockedRequestJsonDataMap.containsKey(entry.getValue()));
        }
    }

    /**
     * Tests if the incoming request jsons are consistent with their corresponding
     * published request json counterparts.
     */
    @Test
    public void testAllRequestJsonsForConsistency() throws Exception {
        Map<String, String> errorsMap = new HashMap<>();
        for (Map.Entry<String, String> entry : requestJsonDataIdMap.entrySet()) {
            String incomingRequestId = entry.getKey();
            String publishedRequestId = entry.getValue();
            MockJsonTestData incomingRequest = mockedRequestJsonDataMap.get(incomingRequestId);
            MockJsonTestData publishedRequest = mockedRequestJsonDataMap.get(publishedRequestId);

            try {
                Boolean consistencyCheckStatus = consistencyCheckerUtil.isConsistent(incomingRequest.getJsonString(), publishedRequest.getJsonString());
                if(!consistencyCheckStatus) {
                    errorsMap.put(incomingRequestId, "Request did not pass consistency check but no exception was caught.");
                }
            } catch (Exception e) {
                errorsMap.put(incomingRequestId, e.getMessage());
            }
        }
        // if any errors caught then print report and fail test
        if (!errorsMap.isEmpty()) {
            System.out.print("\n\nERRORS:\n");
            printErrors(errorsMap);
            Assert.fail();
        }
    }

    private void printErrors(Map<String, String> errorsMap) {
        StringBuilder builder = new StringBuilder();
        builder.append("\nConsistencyCheckerUtil failures summary:\n");
        for (Map.Entry<String, String> entry : errorsMap.entrySet()) {
            builder.append("\n\tRequest id: ")
                    .append(entry.getKey())
                    .append("\n")
                    .append(entry.getValue())
                    .append("\n");
        }
        System.out.println(builder.toString());
    }
}
