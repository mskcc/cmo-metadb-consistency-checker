package org.mskcc.cmo.metadb.service.impl;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.nats.client.Message;
import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.Date;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.Phaser;
import java.util.concurrent.TimeUnit;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.mskcc.cmo.common.FileUtil;
import org.mskcc.cmo.common.MetadbJsonComparator;
import org.mskcc.cmo.messaging.Gateway;
import org.mskcc.cmo.messaging.MessageConsumer;
import org.mskcc.cmo.metadb.model.ConsistencyCheckerRequest;
import org.mskcc.cmo.metadb.model.ConsistencyCheckerRequest.StatusType;
import org.mskcc.cmo.metadb.service.MessageHandlingService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

/**
 *
 * @author ochoaa
 */
@Service
public class MessageHandlingServiceImpl implements MessageHandlingService {
    private static final Log LOG = LogFactory.getLog(MessageHandlingServiceImpl.class);
    private final String TIMESTAMP_FORMAT = "yyyy.MM.dd.HH.mm.ss";

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

    @Value("${messaging.time_threshold_seconds:300}")
    private Integer messagingTimeThreshold;

    @Autowired
    private Gateway messagingGateway;

    @Autowired
    private MetadbJsonComparator metadbJsonComparator;

    private File loggerFile;

    @Autowired
    private void initFileUtilLogger() throws Exception {
        ConsistencyCheckerRequest header = new ConsistencyCheckerRequest();
        this.loggerFile = fileUtil.getOrCreateFileWithHeader(
                consistencyCheckerFailuresFilepath,
                header.getConsistencyCheckerFileHeader());
    }

    // for tracking messages received for each respective topic
    private ConcurrentMap<String, ConsistencyCheckerRequest> igoNewRequestMessagesReceived =
            new ConcurrentHashMap<>();
    private ConcurrentMap<String, ConsistencyCheckerRequest> consistencyCheckerMessagesReceived =
            new ConcurrentHashMap<>();
    // blocking queues for message handling of each topic
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
            setupConsistencyCheckerSubscriber(messagingGateway, this);
            setupIgoNewRequestSubscriber(messagingGateway, this);
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
            LOG.warn("Shutdown initiated, not accepting request: " + request);
            throw new IllegalStateException("Shutdown initiated, not handling any more requests");
        }
    }

    private void setupIgoNewRequestSubscriber(Gateway gateway, MessageHandlingService messageHandlingService)
        throws Exception {
        gateway.subscribe(IGO_NEW_REQUEST_TOPIC, Object.class, new MessageConsumer() {
            public void onMessage(Message msg, Object message) {
                LOG.info("Received message on topic: " + IGO_NEW_REQUEST_TOPIC);
                try {
                    String todaysDate = LocalDate.now().format(DateTimeFormatter.ISO_LOCAL_DATE);
                    String incomingRequestJson = mapper.readValue(
                            new String(msg.getData(), StandardCharsets.UTF_8),
                            String.class);
                    String incomingTimestamp = new SimpleDateFormat(TIMESTAMP_FORMAT).format(new Date());
                    String requestId = getRequestIdFromRequestJson(incomingRequestJson);

                    ConsistencyCheckerRequest request =
                            new ConsistencyCheckerRequest(todaysDate, IGO_NEW_REQUEST_TOPIC,
                            requestId, incomingTimestamp, incomingRequestJson);


                    LOG.info("Adding request to 'igoNewRequestMessagesReceived': "
                            + request.getRequestId());
                    messageHandlingService.newIgoRequestHandler(request);

                } catch (Exception e) {
                    LOG.error("Unable to process IGO_NEW_REQUEST message:\n" + message.toString() + "\n", e);
                }
            }
        });
    }

    @Override
    public void newConsistencyCheckerHandler(ConsistencyCheckerRequest request) throws Exception {
        if (!initialized) {
            throw new IllegalStateException("Message Handling Service has not been initialized");
        }
        if (!shutdownInitiated) {
            consistencyCheckerMessagesReceived.put(request.getRequestId(), request);
        } else {
            System.err.printf("Shutdown initiated, not accepting request: %s\n", request);
            throw new IllegalStateException("Shutdown initiated, not handling any more requests");
        }
    }

    private void setupConsistencyCheckerSubscriber(Gateway gateway, MessageHandlingService service)
        throws Exception {
        gateway.subscribe(NEW_REQUEST_CONSISTENCY_CHECK_TOPIC, Object.class, new MessageConsumer() {
            public void onMessage(Message msg, Object message) {
                LOG.info("Received message on topic: " + NEW_REQUEST_CONSISTENCY_CHECK_TOPIC);
                try {
                    String todaysDate = LocalDate.now().format(DateTimeFormatter.ISO_LOCAL_DATE);
                    String incomingRequestJson = mapper.readValue(
                            new String(msg.getData(), StandardCharsets.UTF_8),
                            String.class);
                    String incomingTimestamp = new SimpleDateFormat(TIMESTAMP_FORMAT).format(new Date());
                    String requestId = getRequestIdFromRequestJson(incomingRequestJson);

                    ConsistencyCheckerRequest request =
                            new ConsistencyCheckerRequest(todaysDate, NEW_REQUEST_CONSISTENCY_CHECK_TOPIC,
                            requestId, incomingTimestamp, incomingRequestJson);

                    LOG.info("Running consistency check on request: " + requestId);
                    if (metadbJsonComparator.isConsistent(incomingRequestJson, incomingRequestJson)) {
                        LOG.info("Consistency check passed, adding to requestPublishingQueue");
                        service.newConsistencyCheckerHandler(request);
                    } else {
                        LOG.warn("Consistency check failed for request: " + requestId);
                        request.setStatusType(StatusType.FAILED_INCONSISTENT_REQUEST_JSONS);
                        fileUtil.writeToFile(loggerFile, request.toString() + "\n");
                    }
                } catch (Exception e) {
                    LOG.error("Unable to process NEW_REQUEST_CONSISTENCY_CHECK_TOPIC message:\n"
                            + message.toString() + "\n", e);
                }
            }
        });
    }

    /**
     * Checks messages received on subscribed topics and adds request to consistency
     * checker queue if the same request has been received on both topics.
     * @throws ParseException
     */
    private void addRequestsToConsistencyCheckerQueue() throws ParseException {
        for (String incomingRequestId : igoNewRequestMessagesReceived.keySet()) {
            LOG.debug("Checking if request is ready for consistency checking: " + incomingRequestId);
            ConsistencyCheckerRequest igoNewRequest = igoNewRequestMessagesReceived.get(incomingRequestId);
            if (consistencyCheckerMessagesReceived.containsKey(incomingRequestId)) {
                LOG.debug("Found same request in both queues: " + incomingRequestId);

                // add to consistency checking queue if request is in both sets of messages received
                ConsistencyCheckerRequest consistencyCheckRequest =
                        consistencyCheckerMessagesReceived.get(incomingRequestId);

                // outgoing json is the json from metadb consistency checker topic
                igoNewRequest.setOutgoingJson(consistencyCheckRequest.getIncomingJson());

                // compare incoming vs outgoing timestamps to determine if time between message
                // received on both topics is greater than specified messaging time threshold
                Date incomingTimestamp = new SimpleDateFormat(TIMESTAMP_FORMAT)
                        .parse(igoNewRequest.getIncomingTimestamp());
                Date outgoingTimestamp = new SimpleDateFormat(TIMESTAMP_FORMAT)
                        .parse(consistencyCheckRequest.getIncomingTimestamp());
                if ((incomingTimestamp.getTime() - outgoingTimestamp.getTime()) > messagingTimeThreshold) {
                    igoNewRequest.setStatusType(StatusType.SUCCESSFUL_PUBLISHING_TIME_EXCEEDED);
                }

                // remove request from igo messages received and add request to consistency checker queue
                igoNewRequestMessagesReceived.remove(incomingRequestId);
                requestConsistencyCheckingQueue.add(igoNewRequest);
            } else {
                // have not gotten the cmo metadb request for consistency checking yet
                // determine if the difference between the current time  and the igo
                // new request incoming timestamp is greater than specified messaging time threshold
                // if status is already set as StatusType.FAILED_DROPPED_MESSAGE then assume
                // we already know that the message has been dropped, no need to log it again
                if (igoNewRequest.getStatusType().equals(
                        ConsistencyCheckerRequest.StatusType.FAILED_DROPPED_MESSAGE)) {
                    continue;
                }
                igoNewRequest.setStatusType(
                        ConsistencyCheckerRequest.StatusType.FAILED_DROPPED_MESSAGE);
                // update status type in igo new requests concurrent map
                igoNewRequestMessagesReceived.put(incomingRequestId, igoNewRequest);
                try {
                    fileUtil.writeToFile(loggerFile, igoNewRequest.toString() + "\n");
                } catch (IOException e) {
                    LOG.error("Error occured during attempt to write "
                            + "to MetaDB consistency checker failures file", e);
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
                    ConsistencyCheckerRequest request =
                            requestPublishingQueue.poll(100, TimeUnit.MILLISECONDS);
                    if (request != null) {
                        LOG.info("Publishing request to topic '" + CMO_NEW_REQUEST_TOPIC
                                + "' with request id: " + request.getRequestId());
                        messagingGateway.publish(request.getRequestId(),
                                CMO_NEW_REQUEST_TOPIC, request.getOutgoingJson());

                        // update outgoing timestamp and compare to incoming to update status type if needed
                        request.setOutgoingTimestamp(
                                new SimpleDateFormat(TIMESTAMP_FORMAT).format(new Date()));
                        Date incomingDate = new SimpleDateFormat(TIMESTAMP_FORMAT)
                                .parse(request.getIncomingTimestamp());
                        Date outgoingDate = new SimpleDateFormat(TIMESTAMP_FORMAT)
                                .parse(request.getOutgoingTimestamp());
                        if ((outgoingDate.getTime() - incomingDate.getTime()) > messagingTimeThreshold) {
                            request.setStatusType(StatusType.SUCCESSFUL_PUBLISHING_TIME_EXCEEDED);
                        }

                        // remove request from consistency check messages received
                        LOG.debug("Removing request from consistency check messages received: "
                                + request.getRequestId());
                        consistencyCheckerMessagesReceived.remove(request.getRequestId());

                        // save request details to logger file
                        fileUtil.writeToFile(loggerFile, request.toString() + "\n");
                    }
                    if (interrupted && requestPublishingQueue.isEmpty()) {
                        break;
                    }
                } catch (InterruptedException e) {
                    interrupted = true;
                } catch (Exception e) {
                    LOG.error("Error encountered during publishing step", e);
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
                    ConsistencyCheckerRequest requestsToCheck =
                            requestConsistencyCheckingQueue.poll(100, TimeUnit.MILLISECONDS);
                    if (requestsToCheck != null) {
                        LOG.info("Running consistency check on request: " + requestsToCheck.getRequestId());
                        // consistency check the requests
                        Boolean passedConsistencyCheck = metadbJsonComparator.isConsistent(
                                requestsToCheck.getIncomingJson(),
                                requestsToCheck.getOutgoingJson());
                        if (passedConsistencyCheck) {
                            LOG.info("Request passed consistency check and adding to publishing queue: "
                                    + requestsToCheck.getRequestId());
                            requestsToCheck.setStatusType(ConsistencyCheckerRequest.StatusType.SUCCESSFUL);
                            // only add request to publishing queue if it passed the consistency check
                            requestPublishingQueue.add(requestsToCheck);
                        } else {
                            LOG.warn("Request failed consistency check: " + requestsToCheck.getRequestId()
                                    + ", storing details to consistency check failures log file");
                            requestsToCheck.setStatusType(
                                    ConsistencyCheckerRequest.StatusType.FAILED_INCONSISTENT_REQUEST_JSONS);

                            // save details to publishing failure logger
                            fileUtil.writeToFile(loggerFile, requestsToCheck.toString() + "\n");
                        }
                    }
                    if (interrupted && requestConsistencyCheckingQueue.isEmpty()) {
                        break;
                    }
                } catch (InterruptedException e) {
                    interrupted = true;
                } catch (Exception e) {
                    LOG.error("Error encountered during consistency checking step", e);
                }
            }
            consistencyCheckerHandlerShutdownLatch.countDown();
        }
    }

}
