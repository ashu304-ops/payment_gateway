# High-Performance C++ Payment Gateway Engine

A zero-allocation, ultra-low-latency payment processing engine built in C++17 for high-throughout financial environments. Designed to handle heavy concurrency, enforce strict transaction idempotency across distributed clients, and offload database persistence without blocking the core execution thread.

During local bench testing on Arch Linux, the engine handled **10,000 concurrent transactions in ~69 milliseconds (~144,000 req/sec)** with a median (P50) processing latency of **336 nanoseconds**.

---

## Key Features

* **Sub-Microsecond Core Execution:** Zero heap allocations on the critical path. Core validation and state machine transitions execute in nanoseconds.
* **Sharded Atomic Idempotency Manager:** Uses a 64-shard concurrent hash map to detect duplicate requests before execution, eliminating double-charge risks.
* **Strict Transaction State Machine:** Guarantees atomic, valid state transitions (`CREATED` → `VALIDATING` → `PROCESSING` → `AUTHORIZED` → `CAPTURED` → `SUCCESS`).
* **Non-Blocking Persistence:** Offloads database logging to a dedicated background worker thread using native MySQL/MariaDB C client bindings.
* **Extensible Processor Architecture:** Pluggable, polymorphism-based processors for Cards, Bank Transfers, Online Wallets, and Offline Vouchers.
* **Nanosecond Latency Telemetry:** Built-in percentile tracking (P50, P90, P95, P99, P99.9) measuring queue wait time, core processing, and end-to-end latency.

---

## Performance Summary

* **Throughput:** ~144,500 requests/sec
* **Batch Size:** 10,000 incoming requests
* **Worker Threads:** 8 standard POSIX worker threads
* **Duplicate Detection Rate:** 100% accuracy (2,000 duplicate idempotency keys caught instantly out of 10,000)

| Percentile | Queue Latency | Processing Latency | End-to-End Latency |
| :--- | :--- | :--- | :--- |
| **P50** | 1.40 ms | **336 ns** | 2.15 ms |
| **P90** | 5.33 ms | **534 ns** | 5.73 ms |
| **P95** | 5.72 ms | **669 ns** | 6.42 ms |
| **P99** | 6.03 ms | **1.02 µs** | 7.48 ms |
| **P99.9** | 6.45 ms | **22.70 µs** | 7.50 ms |

---

## Architecture Overview

1. **Ingress:** Transactions are assigned an ingress timestamp and dispatched to a ThreadPool task queue.
2. **Idempotency Guard:** The worker thread checks the `AtomicIdempotencyManager` using a sharded hash key.
   * **First Request (Leader):** Acquires execution rights and proceeds to processing.
   * **Duplicate Request (Follower):** Suspends and waits for the leader thread to finish, returning the cached result directly.
3. **Core Processing:** Validates payload parameters and routes the request through the `ProcessorRegistry`.
4. **State Machine:** Enforces valid lifecycle transitions. Invalid state shifts reject immediately.
5. **Async DB Queue:** The finalized transaction record is pushed into a lock-free queue for background thread database insertion (`INSERT ... ON DUPLICATE KEY UPDATE`).

---

## Prerequisites

To build and run the engine locally, you will need:

* **C++ Compiler:** `g++` or `clang++` supporting **C++17** or higher.
* **MySQL / MariaDB Client Library:**
  * **Arch Linux:** `sudo pacman -S mariadb-libs`
  * **Ubuntu/Debian:** `sudo apt install libmariadb-dev` or `libmysqlclient-dev`
  * **Fedora/RHEL:** `sudo dnf install mariadb-devel`
* **MySQL/MariaDB Server:** Running on `127.0.0.1:3306` (Optional for benchmarks; connection failures log a non-fatal warning).

---

## Database Setup

Run the following SQL snippet in your MySQL server to set up the persistence target:

```sql
CREATE DATABASE IF NOT EXISTS payment_gateway_db;
USE payment_gateway_db;

CREATE TABLE IF NOT EXISTS payment_transactions (
    transaction_id VARCHAR(64) PRIMARY KEY,
    idempotency_key VARCHAR(64) NOT NULL,
    merchant_id VARCHAR(64) NOT NULL,
    amount BIGINT NOT NULL,
    currency VARCHAR(8) NOT NULL,
    payment_method VARCHAR(32) NOT NULL,
    status VARCHAR(32) NOT NULL,
    response_code VARCHAR(32) NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    INDEX idx_idempotency (idempotency_key)
);
