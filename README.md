# RabbitMQ Tester

RabbitMQ golden signals application to test and monitor the availebility of RabbitMQ clusters. This application has two parts; 1) a producer that is producing messages to an fanout exchange, 2) consumers that are reads the messages from a fanout exchange queue. Both producer and consumer are exposing metrics to Prometheus.
