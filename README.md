# PulseStream: Distributed Event-Driven Microservices Platform

[![Java](https://img.shields.io/badge/Java-21-orange.svg)](https://openjdk.org/)
[![Spring Boot](https://img.shields.io/badge/Spring%20Boot-3.4.x-brightgreen.svg)](https://spring.io/projects/spring-boot)
[![Apache Kafka](https://img.shields.io/badge/Apache%20Kafka-3.7%20KRaft-red.svg)](https://kafka.apache.org/)
[![MySQL](https://img.shields.io/badge/MySQL-8.0-blue.svg)](https://www.mysql.com/)
[![Architecture](https://img.shields.io/badge/Architecture-Event--Driven%20%7C%20Saga-purple.svg)]()

> **PulseStream** is a production-grade, distributed event-driven microservices platform engineered with **Spring Boot 3**, **Apache Kafka (KRaft mode)**, and **MySQL 8**. It implements hardened distributed systems patterns to eliminate the dual-write problem, guarantee idempotent event processing, isolate poison pills via non-blocking Dead-Letter Queues (DLQ), and coordinate multi-service business workflows through choreographed sagas with automated compensating transactions.

---

## 1. Architectural Highlights & Distributed Patterns

| Distributed Pattern | Problem Solved | PulseStream Implementation |
| :--- | :--- | :--- |
| **Transactional Outbox Pattern** | Dual-write vulnerability between relational database commits and Kafka broker publishes. | Persists order state and `outbox_events` in a single local database transaction. An asynchronous relay polls pending outbox records and publishes to Kafka with `acks=all`. |
| **Distributed Idempotent Consumer** | Kafka *at-least-once* delivery causes duplicate events during rebalances or network retries. | Consumers check and record unique `eventId` tokens in local `processed_events` tables inside transactional boundaries, guaranteeing zero duplicate debits or reservations. |
| **Non-Blocking Retry & DLQ** | Poison pills and transient consumer failures stall consumer group partition progress. | Uses Spring Kafka `@RetryableTopic` with exponential backoff across isolated retry topics. Unrecoverable failures route directly to `.DLT` queues without halting the main partition. |
| **Choreographed Distributed Saga** | Lack of distributed ACID / 2PC transactions across isolated per-service databases. | Services react asynchronously to domain events (`OrderCreated`, `PaymentCompleted`, `InventoryReserved`). If stock is depleted, an `InventoryFailed` event automatically triggers compensating refunds in Payment Service and order cancellation in Order Service. |
| **Partition-Key In-Order Guarantees** | Concurrent events for the same order arriving out-of-order at downstream consumers. | All order-related events are keyed by `orderId`, guaranteeing strict FIFO log sequencing within the exact same Kafka partition. |

---

## 2. System Architecture & Service Topography

`	ext
                                [ HTTP Client ]
                                       │
                                       ▼ (POST /orders)
                            ┌─────────────────────┐
                            │ pulse-order-service │ (Port 8081)
                            │ DB: pulsestream_order│
                            └──────────┬──────────┘
                                       │ (Transactional Outbox)
                                       ▼
                       ═════════════════════════════
                              KAFKA EVENT LOG
                       ═════════════════════════════
                              ▲               ▲
                              │               │
                              ▼               ▼
                   ┌──────────────────┐    ┌─────────────────────┐
                   │pulse-payment-svc │    │ pulse-inventory-svc │
                   │  (Port 8082)     │    │    (Port 8083)      │
                   │DB:pulsestream_pay│    │ DB: pulsestream_inv │
                   └──────────────────┘    └─────────────────────┘
`

---

## 3. Multi-Module Project Structure

`	ext
pulsestream/
├── pom.xml                     # Multi-module Maven parent POM
├── docker-compose.yml          # Kafka 3.7 KRaft, Kafka UI, MySQL 8 (3 databases)
├── pulse-common-events/        # Shared immutable Java 21 event records & contracts
├── pulse-order-service/        # Order ingress, lifecycle state machine & Outbox Relay (8081)
├── pulse-payment-service/      # Idempotent payment processing & compensating refunds (8082)
└── pulse-inventory-service/    # Stock reservation & Non-blocking Retry/DLQ pipelines (8083)
`

---

## 4. Technology Stack

- **Runtime & Language**: Java 21 LTS
- **Framework**: Spring Boot 3.4.x, Spring Kafka, Spring Data JPA
- **Event Streaming**: Apache Kafka 3.7 (KRaft mode — zero ZooKeeper dependency)
- **Databases**: MySQL 8 (Database-per-service pattern: 3 isolated schemas)
- **Database Migrations**: Flyway 10
- **Testing & Verification**: JUnit 5, Mockito, Testcontainers, Awaitility
- **Container Infrastructure**: Docker Compose, Kafka UI (Port 8085)
