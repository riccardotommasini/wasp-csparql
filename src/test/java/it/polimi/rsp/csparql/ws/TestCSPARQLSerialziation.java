package it.polimi.rsp.csparql.ws;

import com.github.jsonldjava.core.JsonLdError;
import com.github.jsonldjava.core.JsonLdProcessor;
import com.github.jsonldjava.core.RDFDataset;
import com.github.jsonldjava.utils.JsonUtils;
import com.hp.hpl.jena.graph.NodeFactory;
import com.hp.hpl.jena.rdf.model.*;
import com.hp.hpl.jena.rdf.model.impl.ResourceImpl;
import eu.larkc.csparql.cep.api.RdfQuadruple;
import it.polimi.deib.rsp.vocals.rdf4j.VocalsFactoryRDF4J;
import it.polimi.sr.wasp.server.exceptions.ServiceException;
import org.apache.commons.io.IOUtils;
import org.eclipse.rdf4j.model.Model;
import org.eclipse.rdf4j.model.impl.LinkedHashModel;
import org.eclipse.rdf4j.rio.RDFParser;
import org.eclipse.rdf4j.rio.RDFWriter;
import org.eclipse.rdf4j.rio.Rio;
import org.eclipse.rdf4j.rio.helpers.JSONLDMode;
import org.eclipse.rdf4j.rio.helpers.JSONLDSettings;
import org.eclipse.rdf4j.rio.helpers.StatementCollector;
import org.eclipse.rdf4j.rio.jsonld.JSONLDWriterFactory;

import java.io.IOException;
import java.io.InputStream;
import java.io.StringReader;
import java.io.StringWriter;
import java.util.List;
import java.util.Set;

public class TestCSPARQLSerialziation {

    public static void main(String[] args) throws IOException {

        InputStream file = TestCSPARQLSerialziation.class.getClassLoader().getResourceAsStream("input.json");

        StringReader in = new StringReader(IOUtils.toString(file));

        RDFParser rdfParser = Rio.createParser(org.eclipse.rdf4j.rio.RDFFormat.JSONLD);
        Model model = new LinkedHashModel();
        rdfParser.setRDFHandler(new StatementCollector(model));
        rdfParser.parse(in, "http://streamreasoning.org/icwe");


        long milliseconds = System.currentTimeMillis();
        model.stream().map(statement -> new RdfQuadruple(statement.getSubject().stringValue(),
                statement.getPredicate().stringValue()
                , statement.getObject().stringValue(), milliseconds));


        StringWriter out = new StringWriter();
        JSONLDWriterFactory jsonldWriterFactory = new JSONLDWriterFactory();
        RDFWriter rdfWriter = jsonldWriterFactory.getWriter(out);
        VocalsFactoryRDF4J.prefixMap.forEach(rdfWriter::handleNamespace);
        rdfWriter.getWriterConfig().set(JSONLDSettings.JSONLD_MODE, JSONLDMode.COMPACT);
        rdfWriter.startRDF();
        model.forEach(rdfWriter::handleStatement);
        rdfWriter.endRDF();

        System.out.println(out.toString());


    }

    private static RdfQuadruple deserializizeAsJsonSerialization(String asJsonSerialization) {
        try {
            String subject_string = "", predicate_string = "", object_string = "";
            Object jsonObject = null;

            jsonObject = JsonUtils.fromString(asJsonSerialization);

            RDFDataset rd = (RDFDataset) JsonLdProcessor.toRDF(jsonObject);
            Set<String> graphNames = rd.graphNames();
            Resource subject, object = null;
            Property predicate = null;
            RdfQuadruple quadruple = null;
            for (String graphName : graphNames) {

                List<RDFDataset.Quad> list = rd.getQuads(graphName);

                for (RDFDataset.Quad q : list) {
                    predicate_string = q.getPredicate().getValue();
                    predicate = ResourceFactory.createProperty(predicate_string);

                    subject_string = q.getSubject().getValue();
                    subject = !q.getObject().isBlankNode() ? ResourceFactory.createResource(subject_string) : new ResourceImpl(new AnonId(subject_string));

                    object_string = q.getObject().getValue();
                    if (!q.getObject().isLiteral()) {
                        if (!q.getObject().isBlankNode()) {
                            Resource resource = ResourceFactory.createResource(object_string);
                            quadruple = new RdfQuadruple(subject.toString(), predicate.toString(), object.toString(), System.currentTimeMillis());

                        } else {
                            object = new ResourceImpl(new AnonId(object_string));
                            quadruple = new RdfQuadruple(subject.toString(), predicate.toString(), object.toString(), System.currentTimeMillis());
                        }
                    } else {
                        object_string = q.getObject().getDatatype();
                        Literal typedLiteral = ResourceFactory.createTypedLiteral(object_string, NodeFactory.getType(object_string));
                        quadruple = new RdfQuadruple(subject.toString(), predicate.toString(), typedLiteral.toString(), System.currentTimeMillis());
                    }

                }
            }
            return quadruple;
        } catch (IOException | JsonLdError e) {
            e.printStackTrace();
            throw new ServiceException(e.getCause());
        }
    }
}
