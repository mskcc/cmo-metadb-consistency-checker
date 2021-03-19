package org.mskcc.cmo.metadb.util;

/**
 *
 * @author ochoaa
 */
public class ConsistencyCheckerUtil {
    /**
     * Specifications:
     * 
     * - 2 JSONs with identical contents returns true even if JSONs are ordered differently
     * - JSON comparison ignores fields like "[*]metaDbId"
     * - JSON ignores date-like fields that MetaDB introduces
     * - JSON ignores neo4j-generated id values (usually just called Id  fields)
     * 
     */
}
