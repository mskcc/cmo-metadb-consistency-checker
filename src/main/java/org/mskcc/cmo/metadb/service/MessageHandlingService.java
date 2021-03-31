package org.mskcc.cmo.metadb.service;

import org.mskcc.cmo.messaging.Gateway;
import org.mskcc.cmo.metadb.model.ConsistencyCheckerRequest;

public interface MessageHandlingService {
    void initialize(Gateway gateway) throws Exception;
    void newIgoRequestHandler(ConsistencyCheckerRequest request) throws Exception;
    void newConsistencyCheckerHandler(ConsistencyCheckerRequest request) throws Exception;
    void shutdown() throws Exception;
}
