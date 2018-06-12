package it.polimi.rsp.wasp.csparql;

import eu.larkc.csparql.cep.api.RdfStream;
import eu.larkc.csparql.core.engine.CsparqlEngineImpl;
import eu.larkc.csparql.core.engine.CsparqlQueryResultProxy;
import it.polimi.sr.wasp.rsp.RSPEngine;
import it.polimi.sr.wasp.rsp.SPARQLUtils;
import it.polimi.sr.wasp.rsp.model.Query;
import it.polimi.sr.wasp.rsp.model.QueryBody;
import it.polimi.sr.wasp.server.model.concept.Channel;
import lombok.extern.java.Log;

import java.text.ParseException;
import java.util.List;

@Log
public class CSPARQLEngine extends RSPEngine {

    CsparqlEngineImpl engine = new CsparqlEngineImpl();

    public CSPARQLEngine(String name, String base) {
        super(name, base);
        engine.initialize();
    }

    @Override
    protected String[] extractStreams(QueryBody queryBody) {
        List<String> strings = SPARQLUtils.extractStreams(queryBody.body);
        return strings.toArray(new String[strings.size()]);
    }

    @Override
    protected Query handleInternalQuery(String qid, String body, String uri, String source) {
        try {

            if (!body.contains("REGISTER"))
                body = "REGISTER STREAM " + source + " AS " + body;
            List<SPARQLUtils.GraphClauses> strings = SPARQLUtils.extractGraphs(body);

            strings.forEach(graphClauses -> engine.putStaticNamedModel(graphClauses.iri, graphClauses.iri));

            CsparqlQueryResultProxy int_query = engine.registerQuery(body, false);

            CsparqlQueryResultStream out = new CsparqlQueryResultStream(base, uri, qid, int_query);

            CSPARQLQuery csparqlQuery = new CSPARQLQuery(base, qid, body, int_query, out);

            out.apply(csparqlQuery);

            return csparqlQuery;
        } catch (ParseException e) {
            e.printStackTrace();
        }
        return null;
    }

    @Override
    protected Channel handleInternalStream(String s, String s1) {
        return new CsparqlRDFStream(base, s, s1, new CSPARQLInternalSink(engine.registerStream(new RdfStream(s))));
    }

}