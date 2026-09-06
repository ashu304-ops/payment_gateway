# Concurrent Payment Processing Engine

A **C++17 concurrent payment processing engine** designed to demonstrate real-world systems programming concepts including multithreading, idempotency, state machines, asynchronous persistence, and latency measurement.

The project focuses on the **core processing-engine problems** rather than external banking or payment-provider integrations.

---

## 🚀 What This Project Demonstrates

* **C++17** systems programming
* Multithreaded request processing
* Custom **8-worker thread pool**
* Atomic idempotency handling
* **64-shard lock striping** for concurrent access
* Condition-variable based request coordination
* Transaction state machine
* Strategy-based payment processors
* Asynchronous MySQL persistence
* Queue / processing / end-to-end latency measurement
* P50 / P90 / P95 / P99 / P99.9 percentile analysis
* Concurrent workload and duplicate-request testing

---

## 🏗️ Architecture

```text
                    Payment Request
                           │
                           ▼
                  ┌─────────────────┐
                  │   Thread Pool   │
                  │    8 Workers    │
                  └────────┬────────┘
                           │
                           ▼
                  ┌─────────────────┐
                  │   Idempotency   │
                  │   64 Shards     │
                  └────────┬────────┘
                           │
                    ┌──────┴──────┐
                    │             │
                  Leader       Follower
                    │             │
                    ▼             │
             ┌─────────────┐      │
             │ State       │      │
             │ Machine     │      │
             └──────┬──────┘      │
                    │             │
                    ▼             │
             ┌─────────────┐      │
             │  Processor  │      │
             │ Card/Bank/  │      │
             │ Wallet/etc. │      │
             └──────┬──────┘      │
                    │             │
                    ▼             │
                 SUCCESS          │
                    │             │
                    ▼             │
             Commit Result ───────┘
                    │
                    ▼
          Asynchronous MySQL
             Persistence
```

---

## ⚡ Key Design: Idempotency

Payment systems must prevent the same transaction from being processed multiple times when clients retry requests.

This engine uses **64 lock-striped shards**:

```text
Idempotency Key
       │
       ▼
   Hash(key)
       │
       ▼
   ┌───────────┐
   │ 64 Shards │
   └───────────┘
       │
       ▼
 ┌──────────────────┐
 │ Per-shard mutex  │
 │ + unordered_map  │
 └──────────────────┘
```

For a new key:

```text
Request → ACQUIRED_LEADER → Process Payment → Commit Result
```

For a concurrent duplicate:

```text
Request → PENDING_FOLLOWER → Wait → Receive Leader Result
```

For a completed duplicate:

```text
Request → COMPLETED → Return Cached Result
```

This prevents multiple concurrent requests with the same idempotency key from executing the payment processor more than once.

---

## 🧵 Concurrency Model

The engine uses an **8-worker thread pool** instead of creating a new operating-system thread for every request.

```text
                    Request Producers
                           │
                           ▼
                  ┌────────────────┐
                  │  Task Queue    │
                  └───────┬────────┘
                          │
              ┌───────────┼───────────┐
              ▼           ▼           ▼
           Worker 1    Worker 2    Worker N
              │           │           │
              └───────────┴───────────┘
                          │
                          ▼
                   Payment Engine
```

Synchronization uses:

* `std::mutex`
* `std::condition_variable`
* `std::atomic`
* `std::unique_lock`
* `std::lock_guard`

---

## 🔄 Transaction State Machine

The transaction lifecycle is represented using explicit states:

```text
CREATED
   │
   ▼
VALIDATING
   │
   ▼
PROCESSING
   │
   ▼
AUTHORIZED
   │
   ▼
CAPTURED
   │
   ▼
SUCCESS
```

Failure and timeout paths are also represented:

```text
VALIDATING ───────► FAILED

PROCESSING ───────► FAILED
     │
     └─────────────► TIMEOUT

SUCCESS ──────────► REFUND_PENDING
                         │
                         ▼
                      REFUNDED
```

Transitions are validated by `TransactionStateMachine` rather than allowing arbitrary state changes.

---

## 🧩 Extensible Payment Processing

Payment processing uses a common interface:

```cpp
class PaymentProcessor {
public:
    virtual ~PaymentProcessor() = default;

    virtual void process(
        const PaymentRequest& request,
        PaymentResponse& response
    ) const = 0;
};
```

Current processors:

* Card
* Bank Transfer
* Online Wallet
* Offline

The engine selects the appropriate processor through `ProcessorRegistry`.

This keeps the core engine independent of individual payment-processing implementations.

---

## 🗄️ Asynchronous Persistence

Payment processing is separated from database persistence.

```text
Payment Processing
       │
       ▼
  Result Generated
       │
       ▼
  Persistence Queue
       │
       ▼
 Background Worker
       │
       ▼
     MySQL
```

The payment-processing worker does not wait for the database operation to complete.

This demonstrates the separation between:

**request processing latency** and **persistence latency**.

---

## 📊 Latency Measurement

The engine records three latency categories:

### Queue Latency

Time spent waiting before a worker starts processing.

```text
Request Arrival ───────► Worker Pickup
              Queue Latency
```

### Processing Latency

Time spent executing the payment-processing logic.

```text
Processing Start ─────► Processing End
             Processing Latency
```

### End-to-End Latency

Total time from request ingress to completion.

```text
Request Arrival ─────────────► Completion
              E2E Latency
```

The benchmark reports:

```text
P50
P90
P95
P99
P99.9
```

This makes tail latency visible instead of relying only on average latency.

---

## 🧪 Concurrent Workload

The benchmark executes:

```text
Total Requests     : 10,000
Worker Threads     : 8
Unique Idempotency : 8,000
Duplicate Requests : ~2,000
```

The workload intentionally contains duplicate idempotency keys so that concurrent duplicate handling can be measured.

The engine reports:

* Total requests
* Successful transactions
* Failed transactions
* Duplicate requests
* Actual processor executions
* Throughput
* Queue latency
* Processing latency
* End-to-end latency

---

## 🛠️ Technologies

| Technology                | Usage                            |
| ------------------------- | -------------------------------- |
| C++17                     | Core implementation              |
| STL                       | Containers and utilities         |
| `std::thread`             | Multithreading                   |
| `std::mutex`              | Synchronization                  |
| `std::condition_variable` | Thread coordination              |
| `std::atomic`             | Lock-free counters / state flags |
| `std::future`             | Asynchronous task results        |
| MySQL/MariaDB C API       | Persistence                      |
| CMake                     | Build system                     |
| Linux                     | Development environment          |
| Git/GitHub                | Version control                  |

---

## ▶️ Build

### Prerequisites

* Linux
* C++17 compiler
* CMake
* MySQL/MariaDB development libraries
* Running MySQL/MariaDB instance

### Compile

```bash
g++ -std=c++17 -O2 -pthread payment_gateway.cpp \
    $(mysql_config --cflags --libs) \
    -o payment_gateway
```

### Run

```bash
./payment_gateway
```

---

## 🎯 Why I Built This

The goal was to build a C++ project around problems commonly encountered in concurrent backend and systems software:

* How can multiple requests be processed concurrently?
* How can duplicate operations be prevented?
* How can shared state be accessed safely?
* How can worker threads coordinate efficiently?
* How can queueing impact latency?
* How can tail latency be measured?
* How can persistence be separated from the request-processing path?

The project therefore focuses on **concurrency, correctness, performance measurement, and maintainable C++ design**.

---

## 🔍 Engineering Trade-offs

This project intentionally favors clarity and demonstrability of concurrency mechanisms.

Some areas that would be required for a production payment platform include:

* External payment-provider integration
* Durable distributed idempotency storage
* Request authentication and authorization
* Database connection pooling
* Prepared SQL statements
* Retry and dead-letter handling
* Bounded queues and backpressure
* Distributed tracing
* Metrics export
* Reconciliation and webhook processing
* Multi-instance coordination

The project focuses specifically on the **C++ processing-engine layer**.

---

## 📌 Interview Highlights

The most important engineering areas demonstrated by this project are:

**1. Thread Pool**

Why reuse worker threads instead of creating a thread per request?

**2. Idempotency**

How do concurrent requests with the same key avoid duplicate processing?

**3. Lock Striping**

Why use 64 independent shards instead of one global mutex?

**4. Condition Variables**

How do follower requests wait for the leader's result without busy-waiting?

**5. State Machine**

How are invalid transaction-state transitions prevented?

**6. Latency**

Why measure P99 instead of only average latency?

**7. Asynchronous Persistence**

Why should database persistence not unnecessarily block the processing path?

**8. C++ Lifetime Management**

How are worker-thread lifetime and dependent object lifetime coordinated during shutdown?

---

## 👨‍💻 Author

**Ashish Suresh**

C/C++ Software Developer | C++17 | Multithreading | Linux/UNIX | OOP | Design Patterns

GitHub:
`https://github.com/ashu304-ops/payment_gateway`
