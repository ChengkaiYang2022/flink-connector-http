package com.github.yck.connector.http.sink;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpServer;
import org.apache.commons.io.IOUtils;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.StringReader;
import java.net.HttpURLConnection;
import java.net.InetSocketAddress;

public class StartAHttpService {
    public static void main(String[] args) {
        HttpServiceITCase ht = new HttpServiceITCase("/flink/table1", 8080);
        try {
            ht.run();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
class HttpServiceITCase {
    private HttpServer httpServer;
    private String path;
    private Integer port;

    public HttpServiceITCase(String path, Integer port) {
        this.path = path;
        this.port = port;
    }


    public void run() throws IOException {
        HttpServer server = HttpServer.create(new InetSocketAddress(this.port), 0);
        server.createContext(this.path, new HttpHandlerITCase());
        server.setExecutor(null);
        server.start();
    }
}
class HttpHandlerITCase implements HttpHandler {
    private static final String RESPONSE_STRING = "{\"code\": 200,\"message\":\"success\"}";

    @Override
    public void handle(HttpExchange he) throws IOException {
        if (he.getRequestMethod().equalsIgnoreCase("POST")) {
            try {
                InputStream is = he.getRequestBody();
                System.out.println(IOUtils.toString(is));
                OutputStream os = he.getResponseBody();
                he.sendResponseHeaders(HttpURLConnection.HTTP_OK, RESPONSE_STRING.length());
                os.write(IOUtils.toByteArray(new StringReader(RESPONSE_STRING), "UTF-8"));

                he.close();

            } catch (NumberFormatException | IOException e) {
                System.err.println(e);
            }
        }

    }
}