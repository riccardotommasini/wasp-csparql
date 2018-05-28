package it.polimi.rsp.wasp.csparql;

import it.polimi.deib.rsp.vocals.rdf4j.VocalsFactoryRDF4J;
import it.polimi.sr.wasp.rsp.RSPServer;
import it.polimi.sr.wasp.server.model.persist.StatusManager;
import lombok.extern.java.Log;
import spark.Spark;

import java.io.IOException;

@Log
public class CSPARQLServer extends RSPServer {

    public CSPARQLServer() throws IOException {
        super(new VocalsFactoryRDF4J());
    }

    public static void main(String[] args) throws IOException {
        CSPARQLEngine csparql = new CSPARQLEngine("csparql", "http://localhost:8181");
        if (args.length > 0) {
            new CSPARQLServer().start(csparql, args[0]);
            log.info("Running at http://localhost:8181/csparql");
        } else {
            new CSPARQLServer().start(csparql, CSPARQLServer.class.getResource("default.properties").getPath());
            log.info("Running at http://localhost:8181/csparql");
        }
    }

    @Override
    protected void ingnite(String host, String name, int port) {
        super.ingnite(host, name, port);
        Spark.get(name + "/observers", (request, response) -> StatusManager.sinks.values());
    }
}
