package it.polimi.deib.rsp.ws.csparql;

import eu.larkc.csparql.common.RDFTable;
import eu.larkc.csparql.common.RDFTuple;
import eu.larkc.csparql.core.engine.CsparqlQueryResultProxy;
import it.polimi.sr.wasp.rsp.model.StatelessDataChannel;
import org.eclipse.rdf4j.model.Model;
import org.eclipse.rdf4j.model.Resource;
import org.eclipse.rdf4j.model.ValueFactory;
import org.eclipse.rdf4j.model.impl.SimpleValueFactory;
import org.eclipse.rdf4j.model.util.ModelBuilder;
import org.eclipse.rdf4j.model.util.URIUtil;
import org.eclipse.rdf4j.model.vocabulary.*;
import org.eclipse.rdf4j.rio.RDFWriter;
import org.eclipse.rdf4j.rio.helpers.JSONLDMode;
import org.eclipse.rdf4j.rio.helpers.JSONLDSettings;
import org.eclipse.rdf4j.rio.jsonld.JSONLDWriterFactory;

import java.io.StringWriter;

//This is the outgoing channel that push data to the sinks
public class CsparqlQueryResultStream extends StatelessDataChannel {
    ValueFactory vf = SimpleValueFactory.getInstance();

    public CsparqlQueryResultStream(String base, String id, String uri, CsparqlQueryResultProxy resultProxy) {
        super(base, id, uri);
        resultProxy.addObserver((o, arg) -> put(RDFTableToList((RDFTable) arg)));
    }

    private String RDFTableToList(RDFTable table) {
        ModelBuilder g = new ModelBuilder();

        table.getTuples().forEach((RDFTuple t) ->
                g.add(getResource(t.get(0)),
                        vf.createIRI(t.get(1)),
                        getResource(t.get(2))));
        Model model = g.build();

        StringWriter out = new StringWriter();
        JSONLDWriterFactory jsonldWriterFactory = new JSONLDWriterFactory();
        RDFWriter rdfWriter = jsonldWriterFactory.getWriter(out);
        rdfWriter.handleNamespace(FOAF.PREFIX, FOAF.NAMESPACE);
        rdfWriter.handleNamespace(XMLSchema.PREFIX, XMLSchema.NAMESPACE);
        rdfWriter.handleNamespace(RDF.PREFIX, RDF.NAMESPACE);
        rdfWriter.handleNamespace(RDFS.PREFIX, RDFS.NAMESPACE);
        rdfWriter.handleNamespace(OWL.PREFIX, OWL.NAMESPACE);
        rdfWriter.handleNamespace(FOAF.PREFIX, FOAF.NAMESPACE);
        rdfWriter.getWriterConfig().set(JSONLDSettings.JSONLD_MODE, JSONLDMode.COMPACT);
        rdfWriter.startRDF();
        model.forEach(rdfWriter::handleStatement);
        rdfWriter.endRDF();
        String s = out.toString().replace("\\", "");
        return s;
    }

    private Resource getResource(String iri) {
        return URIUtil.isValidURIReference(iri) ? vf.createIRI(iri) : vf.createBNode();
    }

}
