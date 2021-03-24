package org.mskcc.cmo.metadb.service.impl;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.File;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.Phaser;
import java.util.concurrent.TimeUnit;
import org.mskcc.cmo.common.FileUtil;
import org.mskcc.cmo.messaging.Gateway;
import org.mskcc.cmo.messaging.MessageConsumer;
import org.mskcc.cmo.metadb.logger.ConsistencyCheckerLogger;
import org.mskcc.cmo.metadb.service.MessageHandlingService;
import org.mskcc.cmo.metadb.util.ConsistencyCheckerUtil;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

/**
 *
 * @author ochoaa
 */
@Service
public class MessageHandlingServiceImpl implements MessageHandlingService {

    @Autowired
    private FileUtil fileUtil;

    @Value("${consistency_checker.request_handling_failures_filepath}")
    private String consistencyCheckerFailuresFilepath;

    @Value("${igo.new_request_topic}")
    private String IGO_NEW_REQUEST_TOPIC;

    @Value("${metadb.new_request_consistency_check_topic}")
    private String NEW_REQUEST_CONSISTENCY_CHECK_TOPIC;

    @Value("${metadb.cmo_new_request_topic}")
    private String CMO_NEW_REQUEST_TOPIC;

    @Value("${num.new_request_handler_threads}")
    private int NUM_NEW_REQUEST_HANDLERS;

    @Value("${num.consistency_checker_handler_threads}")
    private int NUM_CONSISTENCY_CHECKER_HANDLERS;

    @Autowired
    private Gateway messagingGateway;

    @Autowired
    private ConsistencyCheckerUtil consistencyCheckerUtil;

    private Map<String, ConsistencyCheckerLogger> igoNewRequestMessagesReceived = new HashMap<>();
    private Map<String, ConsistencyCheckerLogger> cmoNewRequestConsistencyCheckMessagesReceived = new HashMap<>();

    private static final BlockingQueue<ConsistencyCheckerLogger> requestConsistencyCheckingQueue =
        new LinkedBlockingQueue<ConsistencyCheckerLogger>();

    private static final BlockingQueue<ConsistencyCheckerLogger> requestPublishingQueue =
    new LinkedBlockingQueue<ConsistencyCheckerLogger>();
    private static CountDownLatch consistencyCheckerHandlerShutdownLatch;
    private static CountDownLatch newRequestHandlerShutdownLatch;

    private static boolean initialized = false;
    private static volatile boolean shutdownInitiated;
    private static final ExecutorService exec = Executors.newCachedThreadPool();

    private final ObjectMapper mapper = new ObjectMapper();

    @Override
    public void initialize(Gateway gateway) throws Exception {
        if (!initialized) {
            messagingGateway = gateway;
            setupIgoNewRequestHandler(messagingGateway, this);
            initializeNewRequestHandlers();
            initialized = true;
        } else {
            System.err.printf("Messaging Handler Service has already been initialized, ignoring request.\n");
        }
    }

    private void initializeNewRequestHandlers() throws Exception {
        newRequestHandlerShutdownLatch = new CountDownLatch(NUM_NEW_REQUEST_HANDLERS);
        final Phaser newRequestPhaser = new Phaser();
        newRequestPhaser.register();
        for (int lc = 0; lc < NUM_NEW_REQUEST_HANDLERS; lc++) {
            newRequestPhaser.register();
            exec.execute(new NewIgoRequestHandler(newRequestPhaser));
        }
        newRequestPhaser.arriveAndAwaitAdvance();

        consistencyCheckerHandlerShutdownLatch = new CountDownLatch(NUM_CONSISTENCY_CHECKER_HANDLERS);
        final Phaser consistencyCheckerPhaser = new Phaser();
        for (int lc = 0; lc < NUM_CONSISTENCY_CHECKER_HANDLERS; lc++) {
            consistencyCheckerPhaser.register();
            exec.execute(new ConsistencyCheckerHandler(newRequestPhaser));
        }
        consistencyCheckerPhaser.arriveAndAwaitAdvance();
    }

    private String getRequestIdFromRequestJson(String requestJson) throws JsonProcessingException {
        Map<String, Object> requestJsonMap = mapper.readValue(requestJson, Map.class);
        return requestJsonMap.get("requestId").toString();
    }

    @Override
    public void newIgoRequestHandler(ConsistencyCheckerLogger request) throws Exception {
        if (!initialized) {
            throw new IllegalStateException("Message Handling Service has not been initialized");
        }
        if (!shutdownInitiated) {
            igoNewRequestMessagesReceived.put(request.getRequestId(), request);
        } else {
            System.err.printf("Shutdown initiated, not accepting request: %s\n", request);
            throw new IllegalStateException("Shutdown initiated, not handling any more requests");
        }
    }

    private void setupIgoNewRequestHandler(Gateway gateway, MessageHandlingService messageHandlingService)
        throws Exception {
        gateway.subscribe(IGO_NEW_REQUEST_TOPIC, Object.class, new MessageConsumer() {
            public void onMessage(Object message) {
                try {
                    String todaysDate = "current date YYYY/MM/DD";
                    String incomingRequestJson = message.toString();
                    String incomingTimestamp = "current timestamp when subscribe received message (YYYY/MM/DD + HH:MM:SS)";
                    String requestId = getRequestIdFromRequestJson(incomingRequestJson);

                    ConsistencyCheckerLogger request = new ConsistencyCheckerLogger(todaysDate, IGO_NEW_REQUEST_TOPIC,
                            requestId, incomingTimestamp, incomingRequestJson);
                    messageHandlingService.newIgoRequestHandler(request);
                } catch (Exception e) {
                    System.err.printf("Cannot process IGO_NEW_REQUEST:\n%s\n", message);
                    System.err.printf("Exception during processing:\n%s\n", e.getMessage());
                    e.printStackTrace();
                }
            }
        });
    }

    @Override
    public void newRequestConsistencyCheckerHandler(ConsistencyCheckerLogger request) throws Exception {
        if (!initialized) {
            throw new IllegalStateException("Message Handling Service has not been initialized");
        }
        if (!shutdownInitiated) {
            cmoNewRequestConsistencyCheckMessagesReceived.put(request.getRequestId(), request);
        } else {
            System.err.printf("Shutdown initiated, not accepting request: %s\n", request);
            throw new IllegalStateException("Shutdown initiated, not handling any more requests");
        }
    }

    private void setupNewRequestConsistencyCheckerHandler(Gateway gateway, MessageHandlingService messageHandlingService)
        throws Exception {
        gateway.subscribe(NEW_REQUEST_CONSISTENCY_CHECK_TOPIC, Object.class, new MessageConsumer() {
            public void onMessage(Object message) {
                try {
                    String todaysDate = "current date YYYY/MM/DD";
                    String incomingRequestJson = message.toString();
                    String incomingTimestamp = "current timestamp when subscribe received message (YYYY/MM/DD + HH:MM:SS)";
                    String requestId = getRequestIdFromRequestJson(incomingRequestJson);

                    ConsistencyCheckerLogger request = new ConsistencyCheckerLogger(todaysDate, NEW_REQUEST_CONSISTENCY_CHECK_TOPIC,
                            requestId, incomingTimestamp, incomingRequestJson);
                    messageHandlingService.newRequestConsistencyCheckerHandler(request);
                } catch (Exception e) {
                    System.err.printf("Cannot process IGO_NEW_REQUEST:\n%s\n", message);
                    System.err.printf("Exception during processing:\n%s\n", e.getMessage());
                    e.printStackTrace();
                }
            }
        });
    }

    /**
     * Indicates if matching request ids are in both sets of
     * map tracking what messages have been received from each topic.
     * Removes request from 'igoNewRequestMessagesReceived' and adds to
     * 'requestConsistencyCheckingQueue'.
     * @return
     */
    private void addRequestsToConsistencyCheckerQueue() {
        for (String incomingRequestId : igoNewRequestMessagesReceived.keySet()) {
            ConsistencyCheckerLogger igoNewRequest = igoNewRequestMessagesReceived.remove(incomingRequestId);
            if (cmoNewRequestConsistencyCheckMessagesReceived.containsKey(incomingRequestId)) {
                // add to consistency checking queue if request is in both sets of
                // messages received

                // not removing from map because we want to use the incoming timestamp to determine
                // whether message took longer than expected to receive from CMO_NEW_REQUEST_CONSISTENCY_CHECKER
                // this request will be removed when it is published to CMO_NEW_REQUEST
                ConsistencyCheckerLogger metadbConsistencyCheckRequest = cmoNewRequestConsistencyCheckMessagesReceived.get(incomingRequestId);
                igoNewRequest.setOutgoingJson(metadbConsistencyCheckRequest.getIncomingJson());

                // request object now has date, incoming timestamp, incoming json, outgoing json, and topic
                // topic is from the igo new request message received
                requestConsistencyCheckingQueue.add(igoNewRequest);
            } else {
                // have not gotten the cmo metadb request for consistency checking yet
                // determine if the difference between the current time  and the igo
                // new request incoming timestamp is greater than threshold
                // if so then assume dropped?
                // if status is already set as StatusType.FAILED_DROPPED_MESSAGE then assume
                // we already know that the message has been dropped, no need to log it again
                if (igoNewRequest.getStatusType().equals(ConsistencyCheckerLogger.StatusType.FAILED_DROPPED_MESSAGE)) {
                    continue;
                }
                igoNewRequest.setStatusType(ConsistencyCheckerLogger.StatusType.FAILED_DROPPED_MESSAGE);
                igoNewRequestMessagesReceived.put(incomingRequestId, igoNewRequest); // update key-value in map
                try {
                    File loggerFile = fileUtil.getOrCreateFileWithHeader(consistencyCheckerFailuresFilepath,
                            igoNewRequest.getConsistencyCheckerFileHeader());
                    fileUtil.writeToFile(loggerFile, igoNewRequest.toString());
                } catch (IOException e) {
                    System.out.println("Error during attempt to write to metadb checker failures filepath");
                    e.printStackTrace();
                }
            }
        }
    }

    @Override
    public void shutdown() throws Exception {
        if (!initialized) {
            throw new IllegalStateException("Message Handling Service has not been initialized");
        }
        exec.shutdownNow();
        newRequestHandlerShutdownLatch.await();
        consistencyCheckerHandlerShutdownLatch.await();
        shutdownInitiated = true;
    }



    /**
     * Handler for adding requests to the publishing queue if passed consistency check
     * or logging to metadb checker failures logger file if consistency check fails.
     */
    private class NewIgoRequestHandler implements Runnable {

        final Phaser phaser;
        boolean interrupted = false;

        NewIgoRequestHandler(Phaser phaser) {
            this.phaser = phaser;
        }

        @Override
        public void run() {
            phaser.arrive();
            while (true) {
                try {
                    ConsistencyCheckerLogger request = requestPublishingQueue.poll(100, TimeUnit.MILLISECONDS);
                    if (request != null) {
                        messagingGateway.publish(CMO_NEW_REQUEST_TOPIC, request.getOutgoingJson());
                        request.setOutgoingTimestamp("outgoing timestamp");
                        // check difference between incoming timestamp and outgoing
                        // and override status type if the timestamp threshold is greater than expected
                        // time difference = (incoming timestamp - temp outgoing timestamp)
                        //if (time_difference > threshold) {
                        //    request.setStatusType(ConsistencyCheckerLogger.StatusType.SUCCESSFUL_PUBLISHING_TIME_EXCEEDED);
                        //    request.setTopic(CMO_NEW_REQUEST_TOPIC)  if the time exceeded to publish
                        //}

                        // save request details to logger file
                        // request details should have everything now..
                        File loggerFile = fileUtil.getOrCreateFileWithHeader(consistencyCheckerFailuresFilepath,
                                    request.getConsistencyCheckerFileHeader());
                        fileUtil.writeToFile(loggerFile, request.toString());
                    }
                    if (interrupted && requestPublishingQueue.isEmpty()) {
                        break;
                    }
                } catch (InterruptedException e) {
                    interrupted = true;
                } catch (Exception e) {
                    System.err.printf("Error during consistency check: %s\n", e.getMessage());
                    e.printStackTrace();
                }
                newRequestHandlerShutdownLatch.countDown();
            }
        }

    }



    /**
     * Handler for adding requests to the publishing queue if passed consistency check
     * or logging to metadb checker failures logger file if consistency check fails.
     */
    private class ConsistencyCheckerHandler implements Runnable {

        final Phaser phaser;
        boolean interrupted = false;

        ConsistencyCheckerHandler(Phaser phaser) {
            this.phaser = phaser;
        }

        @Override
        public void run() {
            phaser.arrive();
            while (true) {
                addRequestsToConsistencyCheckerQueue();
                try {
                    ConsistencyCheckerLogger requestsToCheck = requestConsistencyCheckingQueue.poll(100, TimeUnit.MILLISECONDS);
                    if (requestsToCheck != null) {
                        // consistency check the requests
                        Boolean passedConsistencyCheck = consistencyCheckerUtil.isConsistent(requestsToCheck.getIncomingJson(),
                                requestsToCheck.getOutgoingJson());
                        if (passedConsistencyCheck) {
                            requestsToCheck.setStatusType(ConsistencyCheckerLogger.StatusType.SUCCESSFUL);
                            // only add request to publishing queue if it passed the consistency check
                            requestPublishingQueue.add(requestsToCheck);
                        } else {
                            requestsToCheck.setStatusType(ConsistencyCheckerLogger.StatusType.FAILED_INCONSISTENT_REQUEST_JSONS);
                            // save details to publishing failure logger
                            File loggerFile = fileUtil.getOrCreateFileWithHeader(consistencyCheckerFailuresFilepath,
                                    requestsToCheck.getConsistencyCheckerFileHeader());
                            fileUtil.writeToFile(loggerFile, requestsToCheck.toString());
                        }
                    }
                    if (interrupted && requestConsistencyCheckingQueue.isEmpty()) {
                        break;
                    }
                } catch (InterruptedException e) {
                    interrupted = true;
                } catch (Exception e) {
                    System.err.printf("Error during consistency check: %s\n", e.getMessage());
                    e.printStackTrace();
                }
            }
            consistencyCheckerHandlerShutdownLatch.countDown();
        }
    }

}
