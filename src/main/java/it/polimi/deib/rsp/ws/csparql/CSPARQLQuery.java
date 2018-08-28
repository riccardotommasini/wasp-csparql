package it.polimi.deib.rsp.ws.csparql;

import eu.larkc.csparql.core.engine.CsparqlQueryResultProxy;
import it.polimi.sr.wasp.rsp.model.InternalTaskWrapper;
import it.polimi.sr.wasp.server.model.concept.Channel;
import lombok.Getter;

import java.util.List;

public class CSPARQLQuery extends InternalTaskWrapper {

    @Getter
    private final CsparqlQueryResultProxy internal_query;

    public CSPARQLQuery(String base, String id, String body, CsparqlQueryResultProxy internal_query, CsparqlQueryResultStream out, List<Channel> istreams) {
        super(id, body, base);
        this.out = out;
        this.internal_query = internal_query;
        this.in.addAll(istreams);
    }


}
