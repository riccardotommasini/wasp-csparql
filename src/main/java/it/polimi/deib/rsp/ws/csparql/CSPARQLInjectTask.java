package it.polimi.deib.rsp.ws.csparql;

import eu.larkc.csparql.cep.api.RdfQuadruple;
import eu.larkc.csparql.cep.api.RdfStream;
import it.polimi.sr.wasp.server.model.concept.Channel;
import it.polimi.sr.wasp.server.model.concept.tasks.SynchTask;
import org.eclipse.rdf4j.model.Model;
import org.eclipse.rdf4j.model.impl.LinkedHashModel;
import org.eclipse.rdf4j.rio.RDFParser;
import org.eclipse.rdf4j.rio.Rio;
import org.eclipse.rdf4j.rio.helpers.StatementCollector;

import java.io.IOException;
import java.io.StringReader;
import java.util.stream.Stream;

public class CSPARQLInjectTask implements SynchTask {

    private final RdfStream stream;

    public CSPARQLInjectTask(RdfStream stream) {
        this.stream = stream;
    }

    @Override
    public String iri() {
        return null;
    }

    @Override
    public Channel out() {
        return null;
    }

    @Override
    public Channel[] in() {
        return new Channel[0];
    }

    private Stream<RdfQuadruple> deserializizeAsJsonSerialization(String asJsonSerialization) {
        try {
            StringReader in = new StringReader(asJsonSerialization);
            RDFParser rdfParser = Rio.createParser(org.eclipse.rdf4j.rio.RDFFormat.JSONLD);
            Model model = new LinkedHashModel();
            rdfParser.setRDFHandler(new StatementCollector(model));
            rdfParser.parse(in, "http://streamreasoning.org/icwe");

            long milliseconds = System.currentTimeMillis();

            return model.stream().map(statement -> new RdfQuadruple(statement.getSubject().stringValue(),
                    statement.getPredicate().stringValue()
                    , statement.getObject().stringValue(), milliseconds));

        } catch (IOException e) {
            e.printStackTrace();
        }
        return Stream.empty();
    }

    @Override
    public void await(String m) {
        deserializizeAsJsonSerialization(m).forEach(stream::put);
    }
}
