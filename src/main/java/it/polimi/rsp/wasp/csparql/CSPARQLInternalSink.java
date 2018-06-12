package it.polimi.rsp.wasp.csparql;

import eu.larkc.csparql.cep.api.RdfQuadruple;
import eu.larkc.csparql.cep.api.RdfStream;
import it.polimi.sr.wasp.server.model.concept.*;
import lombok.AllArgsConstructor;
import lombok.RequiredArgsConstructor;
import org.eclipse.rdf4j.model.Model;
import org.eclipse.rdf4j.model.impl.LinkedHashModel;
import org.eclipse.rdf4j.rio.RDFParser;
import org.eclipse.rdf4j.rio.Rio;
import org.eclipse.rdf4j.rio.helpers.StatementCollector;

import java.io.IOException;
import java.io.StringReader;
import java.util.stream.Stream;

@RequiredArgsConstructor
public class CSPARQLInternalSink implements Sink {

    private final RdfStream stream;
    private final Descriptor descr = new DescriptorHashMap();

    @Override
    public void await(Source source, String s) {
        deserializizeAsJsonSerialization(s).forEach(stream::put);
    }

    @Override
    public void await(Channel channel, String s) {
        deserializizeAsJsonSerialization(s).forEach(stream::put);
    }

    @Override
    public Descriptor describe() {
        return descr;
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

    public void message(String dataSerialization) {
        if (dataSerialization != null && !dataSerialization.isEmpty()) {
            deserializizeAsJsonSerialization(dataSerialization);
        }
    }
}
