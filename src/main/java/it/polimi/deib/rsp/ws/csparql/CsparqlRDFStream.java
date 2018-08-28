package it.polimi.deib.rsp.ws.csparql;


import it.polimi.sr.wasp.rsp.model.StatelessDataChannel;

public class CsparqlRDFStream extends StatelessDataChannel {
    public CsparqlRDFStream(String base, String id, String uri, CSPARQLInjectTask csparqlInternalSink) {
        super(base, id, uri);
        this.tasks.add(csparqlInternalSink);
    }
}
