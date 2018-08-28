package it.polimi.deib.rsp.ws.csparql;

import eu.larkc.csparql.cep.api.RdfStream;
import eu.larkc.csparql.core.engine.CsparqlEngineImpl;
import eu.larkc.csparql.core.engine.CsparqlQueryResultProxy;
import it.polimi.sr.wasp.rsp.RSPEngine;
import it.polimi.sr.wasp.rsp.SPARQLUtils;
import it.polimi.sr.wasp.rsp.exceptions.InternalEngineException;
import it.polimi.sr.wasp.rsp.model.InternalTaskWrapper;
import it.polimi.sr.wasp.rsp.model.QueryBody;
import it.polimi.sr.wasp.server.model.concept.Channel;
import lombok.extern.java.Log;

import java.text.ParseException;
import java.util.List;

@Log
public class CSPARQLEngine extends RSPEngine {

    CsparqlEngineImpl engine = new CsparqlEngineImpl();
    private final boolean inference = false;

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
    protected InternalTaskWrapper handleInternalQuery(String qid, String body, String uri, String source, List<Channel> istreams) throws InternalEngineException {
        try {
            if (!body.contains("REGISTER"))
                body = "REGISTER STREAM " + source + " AS " + body;
            List<SPARQLUtils.GraphClauses> strings = SPARQLUtils.extractGraphs(body);

            strings.forEach(graphClauses -> engine.putStaticNamedModel(graphClauses.iri, graphClauses.iri));

            CsparqlQueryResultProxy int_query = engine.registerQuery(body, inference);

            CsparqlQueryResultStream out = new CsparqlQueryResultStream(base, uri, qid, int_query);

            CSPARQLQuery csparqlQuery = new CSPARQLQuery(base, qid, body, int_query, out, istreams);

            out.add(csparqlQuery);

            return csparqlQuery;
        } catch (ParseException e) {
            throw new InternalEngineException(e.getCause());
        }
    }

    @Override
    protected Channel handleInternalStream(String s, String s1) throws InternalEngineException {
        try {
            RdfStream stream = engine.registerStream(new RdfStream(s));
            return new CsparqlRDFStream(base, s, s1, new CSPARQLInjectTask(stream));
        } catch (Exception e) {
            throw new InternalEngineException(e.getCause());
        }
    }

}