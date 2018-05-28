package it.polimi.rsp.wasp.csparql;

import eu.larkc.csparql.core.engine.CsparqlQueryResultProxy;
import it.polimi.sr.wasp.rsp.model.Query;
import lombok.extern.java.Log;

@Log
public class CSPARQLQuery extends Query {

    private final CsparqlQueryResultProxy internal_query;

    public CSPARQLQuery(String iri, String body, CsparqlQueryResultProxy internal_query, CsparqlQueryResultStream out) {
        super(iri, body);
        this.out = out;
        this.internal_query = internal_query;
    }

}
