package it.polimi.rsp.wasp.csparql;

import com.hp.hpl.jena.datatypes.RDFDatatype;
import com.hp.hpl.jena.datatypes.TypeMapper;
import com.hp.hpl.jena.graph.Graph;
import com.hp.hpl.jena.graph.NodeFactory;
import com.hp.hpl.jena.graph.Triple;
import com.hp.hpl.jena.rdf.model.Model;
import com.hp.hpl.jena.rdf.model.ModelFactory;
import com.hp.hpl.jena.sparql.graph.GraphFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.util.ArrayList;
import java.util.List;

public abstract class RDFUtils {

    private static final Logger logger = LoggerFactory.getLogger(RDFUtils.class);


    public static String serializeTriples(List<Triple> triples) {

        long start = System.currentTimeMillis();

        Graph g = GraphFactory.createDefaultGraph();
        for (Triple t : checkTriplesForLiterals(triples)) {
            g.add(t);
        }

        ByteArrayOutputStream os = new ByteArrayOutputStream();

        Model m = ModelFactory.createModelForGraph(g);
        m.write(os, "JSON-LD");

        long end = System.currentTimeMillis();

        logger.debug("Serialization of triples took " + (end - start) + " ms.");

        return os.toString();
    }

    private static List<Triple> checkTriplesForLiterals(List<Triple> triples) {
        List<Triple> checkedTriples = new ArrayList<Triple>();

        for (Triple triple : triples) {
            String[] object = triple.getObject().toString().split("\\^\\^");
            if (object.length == 2) {
                String value = object[0].replace("\"", "");
                RDFDatatype dataType = TypeMapper.getInstance().getTypeByName(object[1]);
                checkedTriples.add(new Triple(triple.getSubject(), triple.getPredicate(), NodeFactory.createLiteral(value, dataType)));
            } else {
                checkedTriples.add(triple);
            }
        }

        return checkedTriples;
    }

}
