package com.qualle.salesup.api;


import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.net.http.HttpResponse.BodyHandlers;
import java.time.Duration;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

public class CrptApi {

    // For testing
    public static void main(String[] args) throws Exception {

        ObjectMapper mapper = new ObjectMapper();
        HttpClient httpClient = HttpClient.newHttpClient();

        CallLimiter callLimiter = new CallLimiter(Duration.ofSeconds(1), 10);

        CrptClient crptClient = new CrptClient(httpClient, mapper, "https://ismp.crpt.ru/api/v3", callLimiter);
        CrptService crptService = new CrptService(crptClient, mapper);

        List<Thread> threads = new ArrayList<>();

        // Generate test requests
        for (long i = 0; i < 100; i++) {
            long id = i;
            Thread thread = new Thread(() -> crptService.sendDocument(id));
            threads.add(thread);
            thread.start();
        }

        for (Thread thread : threads) {
            thread.join();
        }

        callLimiter.shutdown();
        System.exit(0);
    }

    // Example of service with fake document creation
    public static class CrptService {

        CrptClient crptClient;
        ObjectMapper mapper;

        public CrptService(CrptClient crptClient, ObjectMapper mapper) {
            this.crptClient = crptClient;
            this.mapper = mapper;
        }

        public void sendDocument(long id) {
            Document document = buildDocument(id);
            crptClient.sendDocument(document);
        }

        private Document buildDocument(long id) {
            // example of document creation logic or receiving from another service
            try {
                Document document = mapper.readValue(EXAMPLE, Document.class);
                document.doc_id = "" + id; // for test
                return document;
            } catch (JsonProcessingException e) {
                throw new RuntimeException("Exception while parsing document");
            }
        }
    }

    // Example of client with usage of CallLimiter
    public static class CrptClient {

        private final HttpClient httpClient;
        private final ObjectMapper mapper;
        private final String url;
        private final CallLimiter callLimiter;

        public CrptClient(HttpClient httpClient, ObjectMapper mapper, String url, CallLimiter callLimiter) {
            this.httpClient = httpClient;
            this.mapper = mapper;
            this.url = url;
            this.callLimiter = callLimiter;
        }

        public void sendDocument(Document document) {
            callLimiter.wrap(() -> sendDocumentWithoutLimit(document));
        }

        private int sendDocumentWithoutLimit(Document document) {

            try {
                log("Sending request for document creation. Document id = " + document.doc_id); // getDocId()

                String body = mapper.writeValueAsString(document);

                HttpRequest request = HttpRequest.newBuilder()
                    .uri(new URI(url + "/lk/documents/create"))
                    .POST(HttpRequest.BodyPublishers.ofString(body))
                    .build();

                HttpResponse<String> response = httpClient.send(request, BodyHandlers.ofString());

                // Unable to call api, auth for api required, we think it was successful
                log("Document was successfully created");

                return response.statusCode();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new RuntimeException("Creation of document was interrupted", e);
            } catch (Exception e) {
                throw new RuntimeException("Unable to create document", e);
            }
        }
    }

    public static class CallLimiter {

        private final int limit;
        private final AtomicInteger requestCount;
        private final Semaphore semaphore;
        private final ScheduledFuture scheduler;

        public CallLimiter(Duration period, int limit) {
            this.limit = limit;
            requestCount = new AtomicInteger();
            semaphore = new Semaphore(limit, true);
            scheduler = Executors.newScheduledThreadPool(1)
                .scheduleAtFixedRate(this::reset, period.toNanos(), period.toNanos(), TimeUnit.NANOSECONDS);
            log("CallLimiter initialized");
        }

        public <T> T wrap(Callable<T> callable) {
            return new LimitedCall<>(semaphore, callable).call();
        }

        private void reset() {
            log("Reset request count");
            requestCount.set(0);
            semaphore.release(limit - semaphore.availablePermits());
        }

        public void shutdown() {
            if (!this.scheduler.isCancelled()) {
                this.scheduler.cancel(true);
            }
        }

        // For void calls we can use Runnable
        private static class LimitedCall<T> implements Callable<T> {

            private final Semaphore semaphore;
            private final Callable<T> callable;

            public LimitedCall(Semaphore semaphore, Callable<T> callable) {
                this.semaphore = semaphore;
                this.callable = callable;
            }

            @Override
            public T call() {
                try {
                    semaphore.acquire();
                    return callable.call();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new RuntimeException("Call was interrupted", e);
                } catch (Exception e) {
                    throw new RuntimeException("Filed to execute call", e);
                }
            }
        }
    }

    // For test
    private static void log(String message) {
        System.out.println(LocalDateTime.now() + " " + Thread.currentThread().getName() + ": " + message);
    }

    // Use lombok @Builder/@Data, public for test
    public static class Document {
        public Description description;
        public String doc_id;
        public String doc_status;
        public String doc_type;
        public boolean importRequest;
        public String owner_inn;
        public String participant_inn;
        public String producer_inn;
        public String production_date;
        public String production_type;
        public List<Product> products;
        public String reg_date;
        public String reg_number;

        public static class Product {
            public String certificate_document;
            public String certificate_document_date;
            public String certificate_document_number;
            public String owner_inn;
            public String producer_inn;
            public String production_date;
            public String tnved_code;
            public String uit_code;
            public String uitu_code;
        }

        public static class Description {
            public String participantInn;
        }
    }

    private static final String EXAMPLE = """
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
