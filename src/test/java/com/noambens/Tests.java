package com.noambens;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;

import com.noambens.servers.PeerServerImpl;
import com.noambens.servers.GatewayServer;

/**
 * Unit test for simple App.
 */
@TestMethodOrder(MethodOrderer.Random.class)
public class Tests {

    /* GLOBALS */
    private static final int firstPort = 8000;
    private static final int gateway = 8888;

    private GatewayServer gw;
    private List<PeerServerImpl> servers;

    @AfterEach
    public void tearDown() throws IOException {
        if (servers == null) {
            System.out.println();
            return;
        }
        for (PeerServer server : servers) {
            if (server.getServerId() == 0) {
                gw.shutdown();
            } else {
                server.shutdown();
            }
        }
        try {
            Thread.sleep(3000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        System.out.println();
    }

    @Test
    public void javaRunnerException() throws IOException {
        System.out.println("Test: javaRunnerException");

        // create servers and threads
        Map<Long, InetSocketAddress> peerIDtoAddress = createPeerIDtoAddress(4);
        servers = createServers(peerIDtoAddress);

        assert (verifyLeaders(servers));

        // create client
        List<TestClient> clients = createClients(1);

        // send a message
        String requestBody = "package edu.yu.cs.fall2019.com3800.stage4;\n\npublic class HelloWorld\n{\n    public String run()\n    {\n        return \"Hello, world!\"\n    }\n}\n";
        Response res = sendMessage(requestBody, clients.get(0));
        printResponse(res);

        // validate response
        assertEquals(400, res.getCode());
        assertEquals(false, res.getCached());
    }


    @Test
    public void cacheTest() throws IOException {
        System.out.println("Test: cacheTest");

        // create servers and threads
        Map<Long, InetSocketAddress> peerIDtoAddress = createPeerIDtoAddress(4);
        servers = createServers(peerIDtoAddress);

        assert (verifyLeaders(servers));

        // create client
        List<TestClient> clients = createClients(1);

        // send a message
        String requestBody = "package edu.yu.cs.fall2019.com3800.stage4;\n\npublic class HelloWorld\n{\n    public String run()\n    {\n        return \"Hello, world!\";\n    }\n}\n";
        Response res = sendMessage(requestBody, clients.get(0));
        printResponse(res);

        // validate response
        assertEquals("Hello, world!", res.getBody());
        assertEquals(200, res.getCode());
        assertEquals(false, res.getCached());

        // send the same message again
        Response res2 = sendMessage(requestBody, clients.get(0));
        printResponse(res2);

        // validate response
        assertEquals("Hello, world!", res2.getBody());
        assertEquals(200, res2.getCode());
        assertEquals(true, res2.getCached());
    }

    @Test
    public void manyReqsClientsAndWorkers() throws IOException {
        System.out.println("Test: manyReqsClientsAndWorkers");
        
        // create servers and threads
        Map<Long, InetSocketAddress> peerIDtoAddress = createPeerIDtoAddress(4);
        servers = createServers(peerIDtoAddress);

        assert (verifyLeaders(servers));

        // create client
        List<TestClient> clients = createClients(3);

        String requestBody = "package edu.yu.cs.fall2019.com3800.stage4;\n\npublic class HelloWorld\n{\n    public String run()\n    {\n        return \"Hello, world!\";\n    }\n}\n";
        for (int i = 0; i < 3; i++) {
            // send a message
            String code = requestBody.replace("world!", "world! from code version " + i);
            Response res = sendMessage(code, clients.get(i));
            printResponse(res);

            // validate response
            assertEquals("Hello, world! from code version " + i, res.getBody());
            assertEquals(200, res.getCode());
            assertEquals(false, res.getCached());
        }
    }

    // a single job to submit
    @Test
    public void oneJob() throws IOException {
        System.out.println("Test: oneJob");

        // create servers and threads
        Map<Long, InetSocketAddress> peerIDtoAddress = createPeerIDtoAddress(4);
        servers = createServers(peerIDtoAddress);

        assert (verifyLeaders(servers));

        // create client
        List<TestClient> clients = createClients(1);

        // send a message
        String requestBody = "package edu.yu.cs.fall2019.com3800.stage4;\n\npublic class HelloWorld\n{\n    public String run()\n    {\n        return \"Hello, world!\";\n    }\n}\n";
        Response res = sendMessage(requestBody, clients.get(0));
        printResponse(res);

        // validate response
        assertEquals("Hello, world!", res.getBody());
        assertEquals(200, res.getCode());
        assertEquals(false, res.getCached());

    }

    @Test
    public void invalidHeaders() throws IOException {
        System.out.println("Test: invalidHeaders");

        // create servers and threads
        Map<Long, InetSocketAddress> peerIDtoAddress = createPeerIDtoAddress(4);
        servers = createServers(peerIDtoAddress);

        assert (verifyLeaders(servers));

        // create client
        List<TestClient> clients = createClients(1);

        // send a message
        String requestBody = "package edu.yu.cs.fall2019.com3800.stage4;\n\npublic class HelloWorld\n{\n    public String run()\n    {\n        return \"Hello, world!\";\n    }\n}\n";
        Response res = sendInvalidMessage(requestBody, clients.get(0), Type.CONTENT_TYPE);
        printResponse(res);

        // validate response
        assertEquals(400, res.getCode());
        assertEquals(false, res.getCached());
    }

    @Test
    public void wrongRequestType() throws IOException {
        System.out.println("Test: wrongRequestType");

        // create servers and threads
        Map<Long, InetSocketAddress> peerIDtoAddress = createPeerIDtoAddress(4);
        servers = createServers(peerIDtoAddress);

        assert (verifyLeaders(servers));

        // create client
        List<TestClient> clients = createClients(1);

        // send a message
        String requestBody = "package edu.yu.cs.fall2019.com3800.stage4;\n\npublic class HelloWorld\n{\n    public String run()\n    {\n        return \"Hello, world!\";\n    }\n}\n";
        Response res = sendInvalidMessage(requestBody, clients.get(0), Type.METHOD);
        printResponse(res);

        // validate response
        assertEquals(405, res.getCode());
        assertEquals(false, res.getCached());
    }

    @Test
    public void manyRequestsmanyClients() throws IOException {
        System.out.println("Test: manyRequestsmanyClients");

        // create servers and threads
        Map<Long, InetSocketAddress> peerIDtoAddress = createPeerIDtoAddress(4);
        servers = createServers(peerIDtoAddress);

        assert (verifyLeaders(servers));

        // create client
        List<TestClient> clients = createClients(3);

        String requestBody = "package edu.yu.cs.fall2019.com3800.stage4;\n\npublic class HelloWorld\n{\n    public String run()\n    {\n        return \"Hello, world!\";\n    }\n}\n";
        for (int i = 0; i < 3; i++) {
            // send a message
            String code = requestBody.replace("world!", "world! from code version " + i);
            Response res = sendMessage(code, clients.get(i));
            printResponse(res);

            // validate response
            assertEquals("Hello, world! from code version " + i, res.getBody());
            assertEquals(200, res.getCode());
            assertEquals(false, res.getCached());
        }
    }


    /* HELPER METHODS FOR TESTING */

    // helper test method that checks all servers have the same leader
    private boolean verifyLeaders(List<PeerServerImpl> servers) {
        Vote pastLeader = null;
        for (PeerServer server : servers) {
            Vote leader;
            long time = System.currentTimeMillis();
            while ((leader = server.getCurrentLeader()) == null) {
                if ( System.currentTimeMillis() - time > 10000) {
                    System.out.println("Server " + server.getServerId() + " has state: " + server.getPeerState() +
                        " and has no leader");
                    return false;
                }
            } 
            if (pastLeader != null) {
                if (!pastLeader.equals(leader)) {
                    System.out.println("Server " + server.getServerId() + " has state: " + server.getPeerState() +
                            " and has leader: " + leader.getProposedLeaderID() +
                            ", which is not the same as the past leader: " + pastLeader.getProposedLeaderID());
                    return false;
                } else {
                    System.out.println("Server " + server.getServerId() + " has state: " + server.getPeerState() +
                            " and has leader: " + leader.getProposedLeaderID());
                }
            } else {
                pastLeader = leader;
                System.out.println("Server " + server.getServerId() + " has state: " + server.getPeerState() +
                        " and has leader: " + leader.getProposedLeaderID());
            }
        }
        return true;
    }

    // helper test method that prints out the responses
    private void printResponse(Response res) {
        System.out.println("-----------------------");
        System.out.println("Printing Response:");
        System.out.println("Response code: " + res.getCode());
        System.out.println("Response body: " + res.getBody());
        System.out.println("Response cached: " + res.getCached());
    }

    /* SETUP METHODS */

    // creates a map of peer IDs to addresses
    private Map<Long, InetSocketAddress> createPeerIDtoAddress(int numPeers) {
        Map<Long, InetSocketAddress> peerIDtoAddress = new HashMap<>(numPeers);
        for (int i = 0; i < numPeers; i++) {
            peerIDtoAddress.put((long) i, new InetSocketAddress("localhost", firstPort + (4 * i)));
        }
        return peerIDtoAddress;
    }

    // format the peerIDtoAddress map for individual servers
    private Map<Long, InetSocketAddress> formatPeerIDtoAddress(Map<Long, InetSocketAddress> peerIDtoAddress,
            long serverID) {
        Map<Long, InetSocketAddress> formatted = new HashMap<>(peerIDtoAddress);
        formatted.remove(serverID);
        return formatted;
    }

    // create servers and return a list of them
    private List<PeerServerImpl> createServers(Map<Long, InetSocketAddress> peerIDtoAddress) throws IOException {
        List<PeerServerImpl> servers = new ArrayList<>(peerIDtoAddress.size());

        // gateway has to be serverID 0.
        for (Map.Entry<Long, InetSocketAddress> entry : peerIDtoAddress.entrySet()) {
            // dodging the gateway, which will be created by the GatewayServer constructor
            if (entry.getKey() == 0L) {
                createGateway(peerIDtoAddress);
                servers.add(gw.getPeerServer());
            } else {
                // otherwise, create server
                PeerServerImpl server = new PeerServerImpl(entry.getValue().getPort(),
                        0, entry.getKey(),
                        formatPeerIDtoAddress(peerIDtoAddress, entry.getKey()),
                        0L,
                        0);
                servers.add(server);
                new Thread(server, "Server on port " + server.getAddress().getPort()).start();
            }
        }
        return servers;
    }

    // create the gateway server
    private void createGateway(Map<Long, InetSocketAddress> peerIDtoAddress) throws IOException {
        gw = new GatewayServer(gateway,
                firstPort,
                0L,
                0L,
                new ConcurrentHashMap<Long, InetSocketAddress>(formatPeerIDtoAddress(peerIDtoAddress, firstPort)),
                1);
        gw.start();
        try {
            Thread.sleep(100);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    // create clients
    private List<TestClient> createClients(int numClients) throws MalformedURLException {
        List<TestClient> clients = new ArrayList<>();
        for (int i = 0; i < numClients; i++) {
            clients.add(new TestClient("localhost", gateway));
        }
        return clients;
    }

    // send a message with a given source string and client
    private Response sendMessage(String src, TestClient client) throws IOException {
        // send the request
        client.sendCompileAndRunRequest(src);

        // get the response
        HttpResponse<String> res = null;
        long time = System.currentTimeMillis();

        // spin until we get a response
        while (System.currentTimeMillis() - time < 10000) {
            try {
                Thread.sleep(100);
            } catch (InterruptedException e) {
                throw new IOException(e);
            }

            res = client.getResponse();
            if (res != null) {
                break;
            }
        }

        if (res != null) {
            return new Response(res.statusCode(), res.body(),
                    res.headers().firstValue("Cached-Response").orElse("nuh-uh"));
        }
        return null;
    }

    // send a message with a given source string and client, and with the modified
    // headers to break it
    private Response sendInvalidMessage(String src, TestClient client, Type type) throws IOException {
        // send the request
        client.sendInvalidRequest(src, type);

        // get the response
        HttpResponse<String> res = null;
        long time = System.currentTimeMillis();

        // spin until we get a response
        while (System.currentTimeMillis() - time < 5000) {
            try {
                Thread.sleep(100);
            } catch (InterruptedException e) {
                throw new IOException(e);
            }

            res = client.getResponse();
            if (res != null) {
                break;
            }
        }

        if (res != null) {
            return new Response(res.statusCode(), res.body(),
                    res.headers().firstValue("Cached-Response").orElse("nuh-uh"));
        }
        return null;
    }

    /* HELPER CLASSES */

    /**
     * A simple client for sending requests to the server
     */
    public class TestClient {

        private final URI uri;
        private final HttpClient client;
        private final String contentType = "text/x-java-source";
        private volatile HttpResponse<String> response;

        // create the client
        public TestClient(String hostName, int hostPort) throws MalformedURLException {
            // create the URI
            try {
                this.uri = new URI("http", null, hostName, hostPort, "/compileandrun", null, null);
            } catch (URISyntaxException e) {
                throw new MalformedURLException();
            }

            // build the client
            this.client = HttpClient.newHttpClient();
        }

        /**
         * Takes a given string of code and sends it to the server
         * 
         * @param src - the source code to send
         */
        public void sendCompileAndRunRequest(String src) throws IOException {

            // check for valid input
            if (src == null) {
                throw new IllegalArgumentException();
            }

            // create the request
            HttpRequest req = HttpRequest.newBuilder()
                    .uri(this.uri)
                    .POST(HttpRequest.BodyPublishers.ofString(src))
                    .setHeader("Content-Type", contentType)
                    .build();

            // send the request
            try {
                this.response = this.client.send(req, HttpResponse.BodyHandlers.ofString());
            } catch (InterruptedException e) {
                throw new IOException(e);
            }
        }

        public void sendInvalidRequest(String src, Type type) {
            if (type == Type.METHOD) {
                HttpRequest req = HttpRequest.newBuilder()
                        .uri(this.uri)
                        .GET()
                        .setHeader("Content-Type", "text/x-java-source")
                        .build();
                try {
                    this.response = this.client.send(req, HttpResponse.BodyHandlers.ofString());
                } catch (IOException | InterruptedException e) {
                    e.printStackTrace();
                }
            } else if (type == Type.CONTENT_TYPE) {
                HttpRequest req = HttpRequest.newBuilder()
                        .uri(this.uri)
                        .POST(HttpRequest.BodyPublishers.ofString(src))
                        .setHeader("Content-Type", "text/plain")
                        .build();
                try {
                    this.response = this.client.send(req, HttpResponse.BodyHandlers.ofString());
                } catch (IOException | InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }

        /**
         * Returns the response from the server
         * 
         * @return the response from the server
         */
        public HttpResponse<String> getResponse() throws IOException {
            if (this.response == null) {
                return null;
            }
            HttpResponse<String> res = this.response;
            this.response = null;
            return res;
        }
    }

    /**
     * A simple response object
     */
    public class Response {

        private int code;
        private String body;
        private volatile boolean cached;

        /**
         * Creates a new response object
         * 
         * @param code
         * @param body
         */
        public Response(int code, String body) {
            this(code, body, "false");
        }

        /**
         * Creates a new response object
         * 
         * @param code
         * @param body
         * @param cached
         */
        public Response(int code, String body, String cachedVal) {
            this.code = code;
            this.body = body;
            if (cachedVal.equals("true")) {
                this.cached = true;
            } else if (cachedVal.equals("false")) {
                this.cached = false;
            } else {
                throw new IllegalArgumentException("Invalid cached value");
            }
        }

        // getters
        public int getCode() {
            return this.code;
        }

        public String getBody() {
            return this.body;
        }

        public boolean getCached() {
            return this.cached;
        }
    }

    /**
     * A simple enum for the type of request
     */
    public enum Type {
        METHOD, CONTENT_TYPE
    }

}
