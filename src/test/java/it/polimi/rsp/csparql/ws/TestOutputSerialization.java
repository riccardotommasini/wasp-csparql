package it.polimi.rsp.csparql.ws;

import it.polimi.deib.rsp.vocals.rdf4j.VocalsFactoryRDF4J;
import org.apache.commons.io.IOUtils;
import org.eclipse.rdf4j.model.Model;
import org.eclipse.rdf4j.model.impl.LinkedHashModel;
import org.eclipse.rdf4j.rio.RDFFormat;
import org.eclipse.rdf4j.rio.RDFParser;
import org.eclipse.rdf4j.rio.RDFWriter;
import org.eclipse.rdf4j.rio.Rio;
import org.eclipse.rdf4j.rio.helpers.StatementCollector;

import java.io.IOException;
import java.io.InputStream;
import java.io.StringReader;
import java.io.StringWriter;

public class TestOutputSerialization {

    public static void main(String[] args) throws IOException {

        InputStream file = TestCSPARQLSerialziation.class.getClassLoader().getResourceAsStream("output.json");
        StringReader in = new StringReader(IOUtils.toString(file));

        RDFParser rdfParser = Rio.createParser(org.eclipse.rdf4j.rio.RDFFormat.JSONLD);
        Model model = new LinkedHashModel();
        rdfParser.setRDFHandler(new StatementCollector(model));
        rdfParser.parse(in, "http://streamreasoning.org/icwe");

        StringWriter out = new StringWriter();

        RDFWriter rdfWriter = Rio.createWriter(RDFFormat.TURTLE, System.out);
        VocalsFactoryRDF4J.prefixMap.forEach(rdfWriter::handleNamespace);
        rdfWriter.startRDF();
        model.forEach(rdfWriter::handleStatement);
        rdfWriter.endRDF();
    }
}


