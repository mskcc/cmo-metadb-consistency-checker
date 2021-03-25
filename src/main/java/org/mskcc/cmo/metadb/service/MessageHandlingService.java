package org.mskcc.cmo.metadb.service;

import org.mskcc.cmo.messaging.Gateway;
import org.mskcc.cmo.metadb.model.ConsistencyCheckerRequest;

/**
 * ****************************************************************
 *         DO NOT BEGIN YET UNTIL UTIL IS COMPLETE
 * ****************************************************************
 *
 * <p>Methods set up similar to the MetaDB message handling service.
 * Shuts down gracefully after completing queue if an interrupt signal
 * is occurs or messaging service is called to shutdown.
 *
 * <p>Subscribers:
 * - One subscriber for IGO_NEW_REQUEST (coming from LIMS)
 *          > JSON without any modifications or processing from MetaDB, the
 *              exact message that we are getting from LIMS
 * - One subscriber for CONSISTENCY_CHECKER_NEW_REQUEST (coming from MetaDB)
 *          > JSON that we have processed, persisted into graph DB.
 *              The JSON we send out is what we've mapped and fetched from the
 *              Graph DB itself.
 */
public interface MessageHandlingService {
    void initialize(Gateway gateway) throws Exception;
    void newIgoRequestHandler(ConsistencyCheckerRequest request) throws Exception;
    void newMetaDbRequestConsistencyCheckerHandler(ConsistencyCheckerRequest request) throws Exception;
    void shutdown() throws Exception;
}
