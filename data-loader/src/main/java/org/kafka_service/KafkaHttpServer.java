package org.kafka_service;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpServer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;

public class KafkaHttpServer {

  private static final Logger logger = LoggerFactory.getLogger(KafkaHttpServer.class);
  private static final KafkaConsumerService consumerService = new KafkaConsumerService();
  public static final int PORT = 8000;

  public static void main(String[] args) throws Exception {
    logger.info("Starting Kafka HTTP Server on port {}", PORT);
    HttpServer server = HttpServer.create(new InetSocketAddress(PORT), 0);

    // Define the "/topic" endpoint
    server.createContext("/topic", exchange -> {
      long startTime = System.currentTimeMillis();
      String path = exchange.getRequestURI().getPath();
      String query = exchange.getRequestURI().getQuery();

      logger.info("Received request: path={}, query={}", path, query);

      try {
        // Extract topic_name and offset from the path
        String[] pathParts = path.split("/");
        if (pathParts.length < 3) {
          logger.warn("Invalid URL format for request: {}", path);
          sendResponse(exchange, 400, "Invalid URL format. Expected format: /topic/[topic_name]/[offset]?count=N");
          return;
        }

        String topicName = pathParts[2];
        Integer offset = getOffsetOrDefault(exchange, pathParts);
        if (offset == null) return;

        Integer count = getCountOrDefault(exchange, query);
        if (count == null) return;

        logger.info("Fetching messages: topicName={}, offset={}, count={}", topicName, offset, count);

        // Get messages from Kafka and build the response
        String response = consumerService.getMessages(topicName, offset, count);
        sendResponse(exchange, 200, response);

        logger.info("Request successfully processed in {} ms", System.currentTimeMillis() - startTime);
      } catch (Exception e) {
        logger.error("Error processing request: path={}, query={}", path, query, e);
        sendResponse(exchange, 500, "Internal Server Error");
      }
    });

    server.start();
    logger.info("Server started at http://localhost:{}/", PORT);
  }

  private static Integer getOffsetOrDefault(HttpExchange exchange, String[] pathParts) throws IOException {
    int offset = -1;
    if (pathParts.length >= 4) {
      try {
        offset = Integer.parseInt(pathParts[3]);
      } catch (NumberFormatException e) {
        logger.warn("Invalid offset value provided: {}", pathParts[3]);
        sendResponse(exchange, 400, "Invalid offset value. Leave empty for default value.");
        return null;
      }
    }
    logger.debug("Using offset value: {}", offset);
    return offset;
  }

  private static Integer getCountOrDefault(HttpExchange exchange, String query) throws IOException {
    int count = 10; // Default count value
    if (query != null) {
      for (String param : query.split("&")) {
        String[] keyValue = param.split("=");
        if ("count".equals(keyValue[0])) {
          try {
            count = Integer.parseInt(keyValue[1]);
          } catch (NumberFormatException e) {
            logger.warn("Invalid count value provided: {}", keyValue[1]);
            sendResponse(exchange, 400, "Invalid count value. It should be an integer.");
            return null;
          }
        }
      }
    }
    logger.debug("Using count value: {}", count);
    return count;
  }

  // Utility function to return an HTTP response
  private static void sendResponse(HttpExchange exchange, int statusCode, String response) throws IOException {
    logger.debug("Sending response: statusCode={}, response={}", statusCode, response);
    exchange.sendResponseHeaders(statusCode, response.getBytes().length);
    OutputStream os = exchange.getResponseBody();
    os.write(response.getBytes());
    os.close();
  }
}