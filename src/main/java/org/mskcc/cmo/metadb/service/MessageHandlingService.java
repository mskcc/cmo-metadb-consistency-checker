package org.mskcc.cmo.metadb.service;

/**
 *
 * @author ochoaa
 */
public interface MessageHandlingService {
    /**
     * 
     * ****************************************************************
     *         DO NOT BEGIN YET UNTIL UTIL IS COMPLETE
     * ****************************************************************
     *  
     * Methods set up similar to the MetaDB message handling service. 
     * Shuts down gracefully after completing queue if an interrupt signal 
     * is occurs or messaging service is called to shutdown. 
     * 
     * Subscribers:
     * - One subscriber for IGO_NEW_REQUEST (coming from LIMS)
     *          > JSON without any modifications or processing from MetaDB, the 
     *              exact message that we are getting from LIMS
     * - One subscriber for CONSISTENCY_CHECKER_NEW_REQUEST (coming from MetaDB)
     *          > JSON that we have processed, persisted into graph DB. 
     *              The JSON we send out is what we've mapped and fetched from the 
     *              Graph DB itself.
     */
    
}
