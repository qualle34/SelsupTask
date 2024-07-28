package com.qualle.salesup.api;


import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.net.http.HttpResponse.BodyHandlers;
import java.time.Duration;
import java.time.LocalDateTime;
import java.util.Objects;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

public class Main {

    public static void main(String[] args) {

        CallLimiter callLimiter = new CallLimiter(Duration.ofSeconds(10), 30, 1000);
        HttpClient httpClient = HttpClient.newHttpClient();
        CrptClient crptClient = new CrptClient(httpClient, "https://ismp.crpt.ru/api/v3", callLimiter);

        for (int i = 0; i < 100; i ++) {
            crptClient.sendDocument(REQUEST_BODY);
        }
    }

    public static class CrptClient {

        private final HttpClient httpClient;
        private final String url;
        private final CallLimiter callLimiter;

        public CrptClient(HttpClient httpClient, String url, CallLimiter callLimiter) {
            this.httpClient = httpClient;
            this.url = url;
            this.callLimiter = callLimiter;
        }

        public void sendDocument(String document) {
            callLimiter.wrap(() -> sendDocumentWithoutLimit(document));
        }

        private void sendDocumentWithoutLimit(String document) {

            try {
                log("Sending request for document creation");
                HttpRequest request = HttpRequest.newBuilder()
                    .uri(new URI(url + "/lk/documents/create"))
                    .POST(HttpRequest.BodyPublishers.ofString(document))
                    .build();

                HttpResponse<String> response = httpClient.send(request, BodyHandlers.ofString());
//                verify(response);

                log("Document was successfully created");
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new RuntimeException("Creation of document was interrupted", e);
            } catch (Exception e) {
                throw new RuntimeException("Unable to create document", e);
            }
        }

        private void verify(HttpResponse response) { // Should be implemented separately in the interceptor
            if (response.statusCode() != 200) {
                throw new RuntimeException("Request execution failed with code: " + response.statusCode());
            }
        }
    }

    public static class CallLimiter {

        private final int limit;
        private final int queueLimit;
        private final AtomicInteger requestCount;
        private final Queue<Runnable> requestsQueue;
        private final AtomicBoolean useQueue;
        private final ScheduledExecutorService scheduler;

        public CallLimiter(Duration period, int limit, int queueLimit) {
            this.limit = limit;
            this.queueLimit = queueLimit;
            this.requestCount = new AtomicInteger();
            this.useQueue = new AtomicBoolean(false);
            requestsQueue = new ConcurrentLinkedQueue<>();
            scheduler = Executors.newScheduledThreadPool(1);

            scheduler.scheduleAtFixedRate(this::reset, period.toMillis(), period.toMillis(), TimeUnit.MILLISECONDS);
            log("CallLimiter initialized");
        }

        public void wrap(Runnable runnable) {
            if (!useQueue.get() && requestCount.incrementAndGet() <= limit) {
                log("Limit is unreached, execute call");
                runnable.run();
            } else {
                if (requestsQueue.size() > queueLimit) { // to prevent memory overflow in emergency situations
                    throw new IllegalStateException("Unable to save request, queue is full");
                }
                log("Limit is reached, call queued");
                requestsQueue.offer(runnable);
            }
        }

        private void reset() {
            log("Reset request count, starting process queue");
            useQueue.set(true);
            requestCount.set(0);

            while (!requestsQueue.isEmpty() && requestCount.incrementAndGet() <= limit) {
                log("Execute call from queue");
                Runnable runnable = requestsQueue.poll();
                Objects.requireNonNull(runnable).run();
            }

            log("Reset finished");
            useQueue.set(false);
        }
    }

    private static void log(String message) {
        System.out.println(LocalDateTime.now() + ": " + message);
    }

    private static final String REQUEST_BODY = """
            {
            "description": {
                "participantInn": "string"
            },
            "doc_id": "string",
            "doc_status": "string",
            "doc_type": "LP_INTRODUCE_GOODS",
            "importRequest": true,
            "owner_inn": "string",
            "participant_inn": "string",
            "producer_inn": "string",
            "production_date": "2020-01-23",
            "production_type": "string",
            "products": [
                {
                    "certificate_document": "string",
                    "certificate_document_date": "2020-01-23",
                    "certificate_document_number": "string",
                    "owner_inn": "string",
                    "producer_inn": "string",
                    "production_date": "2020-01-23",
                    "tnved_code": "string",
                    "uit_code": "string",
                    "uitu_code": "string"
                }
            ],
            "reg_date": "2020-01-23",
            "reg_number": "string"
        }""";
}
