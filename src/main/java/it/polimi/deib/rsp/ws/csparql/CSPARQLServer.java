package it.polimi.deib.rsp.ws.csparql;

import it.polimi.deib.rsp.vocals.rdf4j.VocalsFactoryRDF4J;
import it.polimi.sr.wasp.rsp.RSPServer;
import it.polimi.sr.wasp.server.model.persist.StatusManager;
import lombok.extern.log4j.Log4j2;
import spark.Spark;

import java.io.IOException;

@Log4j2
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
            new CSPARQLServer().start(csparql, CSPARQLServer.class.getClassLoader().getResource("default.properties").getPath());
            log.info("Running at http://localhost:8181/csparql");
        }
    }

    @Override
    protected void ignite(String host, String name, int port) {
        super.ignite(host, name, port);
        Spark.get(name + "/observers", (request, response) -> StatusManager.sinks.values());
    }
}
