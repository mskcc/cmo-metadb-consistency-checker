package org.mskcc.cmo.metadb.service.impl;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.File;
import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
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
import org.mskcc.cmo.metadb.model.ConsistencyCheckerRequest;
import org.mskcc.cmo.metadb.model.ConsistencyCheckerRequest.StatusType;
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

    private Map<String, ConsistencyCheckerRequest> igoNewRequestMessagesReceived = new HashMap<>();
    private Map<String, ConsistencyCheckerRequest> metadbRequestConsistencyCheckerMessagesReceived = new HashMap<>();

    private static final BlockingQueue<ConsistencyCheckerRequest> requestConsistencyCheckingQueue =
        new LinkedBlockingQueue<ConsistencyCheckerRequest>();

    private static final BlockingQueue<ConsistencyCheckerRequest> requestPublishingQueue =
    new LinkedBlockingQueue<ConsistencyCheckerRequest>();
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
            setupMetaDbRequestConsistencyCheckerHandler(messagingGateway, this);
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
            exec.execute(new ConsistencyCheckerHandler(consistencyCheckerPhaser));
        }
        consistencyCheckerPhaser.arriveAndAwaitAdvance();
    }

    private String getRequestIdFromRequestJson(String requestJson) throws JsonProcessingException {
        Map<String, Object> requestJsonMap = mapper.readValue(requestJson, Map.class);
        return requestJsonMap.get("requestId").toString();
    }

    @Override
    public void newIgoRequestHandler(ConsistencyCheckerRequest request) throws Exception {
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
                System.out.println("RECEIVED MESSAGE ON TOPIC " + IGO_NEW_REQUEST_TOPIC);
                try {
                    String todaysDate = LocalDate.now().format(DateTimeFormatter.ISO_LOCAL_DATE).toString();
                    String incomingRequestJson = message.toString();
                    String incomingTimestamp = new SimpleDateFormat("yyyy.MM.dd.HH.mm.ss").format(new Date());
                    String requestId = getRequestIdFromRequestJson(incomingRequestJson);

                    ConsistencyCheckerRequest request = new ConsistencyCheckerRequest(todaysDate, IGO_NEW_REQUEST_TOPIC,
                            requestId, incomingTimestamp, incomingRequestJson);
                    System.out.println("Adding request to 'igoNewRequestMessagesReceived': " + request.getRequestId());
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
    public void newMetaDbRequestConsistencyCheckerHandler(ConsistencyCheckerRequest request) throws Exception {
        if (!initialized) {
            throw new IllegalStateException("Message Handling Service has not been initialized");
        }
        if (!shutdownInitiated) {
            metadbRequestConsistencyCheckerMessagesReceived.put(request.getRequestId(), request);
        } else {
            System.err.printf("Shutdown initiated, not accepting request: %s\n", request);
            throw new IllegalStateException("Shutdown initiated, not handling any more requests");
        }
    }

    private void setupMetaDbRequestConsistencyCheckerHandler(Gateway gateway, MessageHandlingService messageHandlingService)
        throws Exception {
        gateway.subscribe(NEW_REQUEST_CONSISTENCY_CHECK_TOPIC, Object.class, new MessageConsumer() {
            public void onMessage(Object message) {
                System.out.println("RECEIVED MESSAGE ON TOPIC " + NEW_REQUEST_CONSISTENCY_CHECK_TOPIC);
                try {
                    String todaysDate = LocalDate.now().format(DateTimeFormatter.ISO_LOCAL_DATE).toString();
                    String incomingRequestJson = message.toString();
                    String incomingTimestamp = new SimpleDateFormat("yyyy.MM.dd.HH.mm.ss").format(new Date());
                    String requestId = getRequestIdFromRequestJson(incomingRequestJson);

                    ConsistencyCheckerRequest request = new ConsistencyCheckerRequest(todaysDate, NEW_REQUEST_CONSISTENCY_CHECK_TOPIC,
                            requestId, incomingTimestamp, incomingRequestJson);
                    System.out.println("Adding request to 'metadbRequestConsistencyCheckerMessagesReceived': " + request.getRequestId());
                    messageHandlingService.newMetaDbRequestConsistencyCheckerHandler(request);
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
     * @throws ParseException 
     */
    private void addRequestsToConsistencyCheckerQueue() throws ParseException {
        for (String incomingRequestId : igoNewRequestMessagesReceived.keySet()) {
            System.out.println("Checking if request is ready for consistency checking: " + incomingRequestId);
            ConsistencyCheckerRequest igoNewRequest = igoNewRequestMessagesReceived.get(incomingRequestId);
            if (metadbRequestConsistencyCheckerMessagesReceived.containsKey(incomingRequestId)) {
                // add to consistency checking queue if request is in both sets of
                // messages received
                System.out.println("Found same request in both queues: " + incomingRequestId);
                // not removing from map because we want to use the incoming timestamp to determine
                // whether message took longer than expected to receive from CMO_NEW_REQUEST_CONSISTENCY_CHECKER
                // this request will be removed when it is published to CMO_NEW_REQUEST
                ConsistencyCheckerRequest metadbConsistencyCheckRequest = metadbRequestConsistencyCheckerMessagesReceived.get(incomingRequestId);
                igoNewRequest.setOutgoingJson(metadbConsistencyCheckRequest.getIncomingJson());
                Date igoRequestDate = new SimpleDateFormat("yyyy.MM.dd.HH.mm.ss").parse(igoNewRequest.getIncomingTimestamp());
                Date metaDbRequestDate = new SimpleDateFormat("yyyy.MM.dd.HH.mm.ss").parse(metadbConsistencyCheckRequest.getIncomingTimestamp());
                if ((igoRequestDate.getTime() - metaDbRequestDate.getTime()) > 300) {
                    igoNewRequest.setStatusType(StatusType.SUCCESSFUL_PUBLISHING_TIME_EXCEEDED);
                }
                // request object now has date, incoming timestamp, incoming json, outgoing json, and topic
                // topic is from the igo new request message received
                igoNewRequestMessagesReceived.remove(incomingRequestId);
                requestConsistencyCheckingQueue.add(igoNewRequest);
            } else {
                // have not gotten the cmo metadb request for consistency checking yet
                // determine if the difference between the current time  and the igo
                // new request incoming timestamp is greater than threshold
                // if so then assume dropped?
                // if status is already set as StatusType.FAILED_DROPPED_MESSAGE then assume
                // we already know that the message has been dropped, no need to log it again
                if (igoNewRequest.getStatusType().equals(ConsistencyCheckerRequest.StatusType.FAILED_DROPPED_MESSAGE)) {
                    continue;
                }
                igoNewRequest.setStatusType(ConsistencyCheckerRequest.StatusType.FAILED_DROPPED_MESSAGE);
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
                    ConsistencyCheckerRequest request = requestPublishingQueue.poll(100, TimeUnit.MILLISECONDS);
                    if (request != null) {
                        System.out.println("Request is ready for publishing: " + request.getRequestId());
                        messagingGateway.publish(CMO_NEW_REQUEST_TOPIC, request.getOutgoingJson());
                        request.setOutgoingTimestamp("outgoing timestamp");
                        Date incomingtDate = new SimpleDateFormat("yyyy.MM.dd.HH.mm.ss").parse(request.getIncomingTimestamp());
                        Date outgoingDate = new SimpleDateFormat("yyyy.MM.dd.HH.mm.ss").parse(request.getOutgoingTimestamp());
                        if ((outgoingDate.getTime() - incomingtDate.getTime()) > 300) {
                            request.setStatusType(StatusType.SUCCESSFUL_PUBLISHING_TIME_EXCEEDED);
                        }
                        // check difference between incoming timestamp and outgoing
                        // and override status type if the timestamp threshold is greater than expected
                        // time difference = (incoming timestamp - temp outgoing timestamp)
                        //if (time_difference > threshold) {
                        //    request.setStatusType(ConsistencyCheckerLogger.StatusType.SUCCESSFUL_PUBLISHING_TIME_EXCEEDED);
                        //    request.setTopic(CMO_NEW_REQUEST_TOPIC)  if the time exceeded to publish
                        //}

                        // remove request from consistency check messages received
                        System.out.println("Removing request from consistency check messages received: " + request.getRequestId());
                        metadbRequestConsistencyCheckerMessagesReceived.remove(request.getRequestId());

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
                try {
                    addRequestsToConsistencyCheckerQueue();
                    ConsistencyCheckerRequest requestsToCheck = requestConsistencyCheckingQueue.poll(100, TimeUnit.MILLISECONDS);
                    if (requestsToCheck != null) {
                        System.out.println("Consistency checking request received: " + requestsToCheck.getRequestId());
                        // consistency check the requests
                        Boolean passedConsistencyCheck = consistencyCheckerUtil.isConsistent(requestsToCheck.getIncomingJson(),
                                requestsToCheck.getOutgoingJson());
                        if (passedConsistencyCheck) {
                            System.out.println("Passed consistency check, marking request as passed and adding to request publishing queue");
                            requestsToCheck.setStatusType(ConsistencyCheckerRequest.StatusType.SUCCESSFUL);
                            // only add request to publishing queue if it passed the consistency check
                            requestPublishingQueue.add(requestsToCheck);
                        } else {
                            System.out.println("request FAILED consistency check: " + requestsToCheck.getRequestId());
                            requestsToCheck.setStatusType(ConsistencyCheckerRequest.StatusType.FAILED_INCONSISTENT_REQUEST_JSONS);
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
