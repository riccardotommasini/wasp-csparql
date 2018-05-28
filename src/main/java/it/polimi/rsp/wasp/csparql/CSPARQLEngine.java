package it.polimi.rsp.wasp.csparql;

import eu.larkc.csparql.cep.api.RdfStream;
import eu.larkc.csparql.core.engine.CsparqlEngineImpl;
import eu.larkc.csparql.core.engine.CsparqlQueryResultProxy;
import it.polimi.sr.wasp.rsp.RSPEngine;
import it.polimi.sr.wasp.rsp.model.Query;
import it.polimi.sr.wasp.server.model.concept.Channel;
import lombok.extern.java.Log;

import java.text.ParseException;

@Log
public class CSPARQLEngine extends RSPEngine {

    CsparqlEngineImpl engine = new CsparqlEngineImpl();

    public CSPARQLEngine(String name, String base) {
        super(name, base);
        engine.initialize();
    }

    @Override
    protected Query handleInternalQuery(String qid, String body, String uri, String source) {
        try {

            CsparqlQueryResultProxy int_query = engine.registerQuery(body, false);

            CsparqlQueryResultStream out = new CsparqlQueryResultStream(uri, qid, int_query);

            CSPARQLQuery csparqlQuery = new CSPARQLQuery(qid, body, int_query, out);

            out.apply(csparqlQuery);

            return csparqlQuery;
        } catch (ParseException e) {
            e.printStackTrace();
        }
        return null;
    }

    @Override
    protected Channel handleInternalStream(String s, String s1) {
        return new CsparqlRDFStream(s, s1, new CSPARQLInternalSink(engine.registerStream(new RdfStream(s))));
    }

}