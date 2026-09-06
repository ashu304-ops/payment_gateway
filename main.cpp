#include <iostream>
#include <string>
#include <vector>
#include <memory>
#include <unordered_map>
#include <queue>
#include <thread>
#include <mutex>
#include <condition_variable>
#include <atomic>
#include <chrono>
#include <algorithm>
#include <future>
#include <iomanip>
#include <array>

// Native MySQL / MariaDB Header
#include <mysql/mysql.h>

// ============================================================================
// 1. DOMAIN MODELS & TYPES
// ============================================================================

enum class PaymentMethod : uint8_t {
    CARD = 0,
    BANK_TRANSFER = 1,
    ONLINE_WALLET = 2,
    OFFLINE = 3
};

std::string paymentMethodToString(PaymentMethod method) {
    switch (method) {
        case PaymentMethod::CARD: return "CARD";
        case PaymentMethod::BANK_TRANSFER: return "BANK_TRANSFER";
        case PaymentMethod::ONLINE_WALLET: return "ONLINE_WALLET";
        case PaymentMethod::OFFLINE: return "OFFLINE";
    }
    return "UNKNOWN";
}

enum class TransactionState : uint8_t {
    CREATED,
    VALIDATING,
    PROCESSING,
    AUTHORIZED,
    CAPTURED,
    SUCCESS,
    FAILED,
    TIMEOUT,
    REFUND_PENDING,
    REFUNDED
};

std::string transactionStateToString(TransactionState state) {
    switch (state) {
        case TransactionState::CREATED: return "CREATED";
        case TransactionState::VALIDATING: return "VALIDATING";
        case TransactionState::PROCESSING: return "PROCESSING";
        case TransactionState::AUTHORIZED: return "AUTHORIZED";
        case TransactionState::CAPTURED: return "CAPTURED";
        case TransactionState::SUCCESS: return "SUCCESS";
        case TransactionState::FAILED: return "FAILED";
        case TransactionState::TIMEOUT: return "TIMEOUT";
        case TransactionState::REFUND_PENDING: return "REFUND_PENDING";
        case TransactionState::REFUNDED: return "REFUNDED";
    }
    return "UNKNOWN";
}

struct PaymentRequest {
    std::string transaction_id;
    std::string idempotency_key;
    std::string merchant_id;
    int64_t amount{0};
    std::string currency;
    PaymentMethod payment_method{PaymentMethod::CARD};
    std::string account_details;
    
    std::chrono::high_resolution_clock::time_point ingress_timestamp;
};

struct PaymentResponse {
    std::string transaction_id;
    std::string idempotency_key;
    TransactionState status{TransactionState::CREATED};
    std::string response_code;
    std::string message;
    
    bool is_duplicate{false};
    
    uint64_t queue_latency_ns{0};
    uint64_t processing_latency_ns{0};
    uint64_t total_e2e_latency_ns{0};
};

struct DBTransactionRecord {
    std::string transaction_id;
    std::string idempotency_key;
    std::string merchant_id;
    int64_t amount;
    std::string currency;
    std::string payment_method;
    std::string status;
    std::string response_code;
};

// ============================================================================
// 2. MYSQL PERSISTENCE MANAGER (NATIVE MARIADB/MYSQL CLIENT API)
// ============================================================================

class MySQLPersistenceManager {
private:
    std::queue<DBTransactionRecord> record_queue_;
    std::mutex queue_mutex_;
    std::condition_variable cv_;
    std::thread worker_thread_;
    std::atomic<bool> stop_flag_{false};

    std::string db_host_;
    std::string db_user_;
    std::string db_pass_;
    std::string db_schema_;

    void workerLoop() {
        MYSQL* conn = mysql_init(nullptr);
        if (!conn) {
            std::cerr << "[MySQL Error] Could not initialize MySQL handle\n";
            return;
        }

        if (!mysql_real_connect(conn, db_host_.c_str(), db_user_.c_str(), db_pass_.c_str(), 
                                db_schema_.c_str(), 3306, nullptr, 0)) {
            std::cerr << "[MySQL Warning] Connection failed: " << mysql_error(conn) 
                      << " (Ensure DB 'payment_gateway_db' is created)\n";
            mysql_close(conn);
            return;
        }

        while (true) {
            DBTransactionRecord record;
            {
                std::unique_lock<std::mutex> lock(queue_mutex_);
                cv_.wait(lock, [this]() { return !record_queue_.empty() || stop_flag_; });
                if (stop_flag_ && record_queue_.empty()) break;
                
                record = std::move(record_queue_.front());
                record_queue_.pop();
            }

            std::string query = "INSERT INTO payment_transactions "
                "(transaction_id, idempotency_key, merchant_id, amount, currency, payment_method, status, response_code) "
                "VALUES ('" + record.transaction_id + "', '" + record.idempotency_key + "', '" +
                record.merchant_id + "', " + std::to_string(record.amount) + ", '" +
                record.currency + "', '" + record.payment_method + "', '" +
                record.status + "', '" + record.response_code + "') "
                "ON DUPLICATE KEY UPDATE status=VALUES(status), response_code=VALUES(response_code);";

            mysql_query(conn, query.c_str());
        }

        mysql_close(conn);
    }

public:
    MySQLPersistenceManager(std::string host, std::string user, std::string pass, std::string schema)
        : db_host_(std::move(host)), db_user_(std::move(user)), 
          db_pass_(std::move(pass)), db_schema_(std::move(schema)) {
        worker_thread_ = std::thread(&MySQLPersistenceManager::workerLoop, this);
    }

    ~MySQLPersistenceManager() {
        stop_flag_ = true;
        cv_.notify_all();
        if (worker_thread_.joinable()) {
            worker_thread_.join();
        }
    }

    void asyncPersist(DBTransactionRecord record) {
        {
            std::lock_guard<std::mutex> lock(queue_mutex_);
            record_queue_.push(std::move(record));
        }
        cv_.notify_one();
    }
};

// ============================================================================
// 3. STATE MACHINE IMPLEMENTATION
// ============================================================================

class TransactionStateMachine {
public:
    static bool isValidTransition(TransactionState current, TransactionState next) {
        switch (current) {
            case TransactionState::CREATED:
                return next == TransactionState::VALIDATING;
            case TransactionState::VALIDATING:
                return next == TransactionState::PROCESSING || next == TransactionState::FAILED;
            case TransactionState::PROCESSING:
                return next == TransactionState::AUTHORIZED || 
                       next == TransactionState::CAPTURED || 
                       next == TransactionState::FAILED || 
                       next == TransactionState::TIMEOUT;
            case TransactionState::AUTHORIZED:
                return next == TransactionState::CAPTURED || next == TransactionState::FAILED;
            case TransactionState::CAPTURED:
                return next == TransactionState::SUCCESS || next == TransactionState::FAILED;
            case TransactionState::SUCCESS:
                return next == TransactionState::REFUND_PENDING;
            case TransactionState::REFUND_PENDING:
                return next == TransactionState::REFUNDED || next == TransactionState::SUCCESS;
            case TransactionState::FAILED:
            case TransactionState::TIMEOUT:
            case TransactionState::REFUNDED:
                return false;
        }
        return false;
    }

    static bool transitionTo(TransactionState& current, TransactionState next) {
        if (isValidTransition(current, next)) {
            current = next;
            return true;
        }
        return false;
    }
};

// ============================================================================
// 4. PAYMENT PROCESSORS
// ============================================================================

class PaymentProcessor {
public:
    virtual ~PaymentProcessor() = default;
    virtual void process(const PaymentRequest& request, PaymentResponse& response) const = 0;
};

class CardProcessor final : public PaymentProcessor {
public:
    void process(const PaymentRequest& req, PaymentResponse& res) const override {
        if (req.amount <= 0) {
            res.status = TransactionState::FAILED;
            res.response_code = "ERR_INVALID_AMOUNT";
            res.message = "Transaction amount must be positive";
        } else if (req.amount > 10000000) {
            res.status = TransactionState::FAILED;
            res.response_code = "DECLINED_INSUFFICIENT_FUNDS";
            res.message = "Card limit exceeded";
        } else {
            res.status = TransactionState::AUTHORIZED;
            res.response_code = "00";
            res.message = "Authorization Granted";
        }
    }
};

class BankProcessor final : public PaymentProcessor {
public:
    void process(const PaymentRequest& req, PaymentResponse& res) const override {
        res.status = TransactionState::AUTHORIZED;
        res.response_code = "BANK_00";
        res.message = "Direct Bank Transfer Verified";
    }
};

class OnlineWalletProcessor final : public PaymentProcessor {
public:
    void process(const PaymentRequest& req, PaymentResponse& res) const override {
        res.status = TransactionState::AUTHORIZED;
        res.response_code = "WALLET_200";
        res.message = "Wallet Balance Reserved";
    }
};

class OfflineProcessor final : public PaymentProcessor {
public:
    void process(const PaymentRequest& req, PaymentResponse& res) const override {
        res.status = TransactionState::AUTHORIZED;
        res.response_code = "OFFLINE_RECORDED";
        res.message = "Offline Voucher Generated";
    }
};

class ProcessorRegistry {
private:
    CardProcessor card_proc_;
    BankProcessor bank_proc_;
    OnlineWalletProcessor wallet_proc_;
    OfflineProcessor offline_proc_;

public:
    const PaymentProcessor& getProcessor(PaymentMethod method) const {
        switch (method) {
            case PaymentMethod::CARD: return card_proc_;
            case PaymentMethod::BANK_TRANSFER: return bank_proc_;
            case PaymentMethod::ONLINE_WALLET: return wallet_proc_;
            case PaymentMethod::OFFLINE: return offline_proc_;
        }
        return card_proc_;
    }
};

// ============================================================================
// 5. ATOMIC IDEMPOTENCY ENGINE
// ============================================================================

enum class ReservationStatus {
    ACQUIRED_LEADER,
    PENDING_FOLLOWER,
    COMPLETED
};

class AtomicIdempotencyManager {
public:
    struct Entry {
        bool is_complete{false};
        PaymentResponse result;
        std::condition_variable cv;
    };

private:
    struct MapShard {
        mutable std::mutex mutex;
        std::unordered_map<std::string, std::shared_ptr<Entry>> map;
    };

    static constexpr size_t NUM_SHARDS = 64;
    std::array<MapShard, NUM_SHARDS> shards_;

    size_t getShardIndex(const std::string& key) const {
        return std::hash<std::string>{}(key) % NUM_SHARDS;
    }

public:
    ReservationStatus getOrReserve(const std::string& key, PaymentResponse& out_response, std::shared_ptr<Entry>& out_entry) {
        size_t idx = getShardIndex(key);
        std::unique_lock<std::mutex> lock(shards_[idx].mutex);

        auto it = shards_[idx].map.find(key);
        if (it != shards_[idx].map.end()) {
            auto entry = it->second;
            if (entry->is_complete) {
                out_response = entry->result;
                out_response.is_duplicate = true;
                return ReservationStatus::COMPLETED;
            } else {
                out_entry = entry;
                return ReservationStatus::PENDING_FOLLOWER;
            }
        }

        auto new_entry = std::make_shared<Entry>();
        shards_[idx].map[key] = new_entry;
        out_entry = new_entry;
        return ReservationStatus::ACQUIRED_LEADER;
    }

    void waitAndCopyResult(const std::shared_ptr<Entry>& entry, PaymentResponse& out_response, const std::string& key) {
        size_t idx = getShardIndex(key);
        std::unique_lock<std::mutex> lock(shards_[idx].mutex);
        
        entry->cv.wait(lock, [&entry]() { return entry->is_complete; });
        out_response = entry->result;
        out_response.is_duplicate = true;
    }

    void commitResult(const std::string& key, const std::shared_ptr<Entry>& entry, const PaymentResponse& result) {
        size_t idx = getShardIndex(key);
        {
            std::unique_lock<std::mutex> lock(shards_[idx].mutex);
            entry->result = result;
            entry->is_complete = true;
        }
        entry->cv.notify_all();
    }
};

// ============================================================================
// 6. METRICS MONITOR
// ============================================================================

class MetricsMonitor {
private:
    std::mutex mutex_;
    
    std::vector<uint64_t> queue_latencies_ns_;
    std::vector<uint64_t> processing_latencies_ns_;
    std::vector<uint64_t> e2e_latencies_ns_;

    std::atomic<uint64_t> total_requests_{0};
    std::atomic<uint64_t> success_count_{0};
    std::atomic<uint64_t> failure_count_{0};
    std::atomic<uint64_t> duplicate_requests_count_{0};
    std::atomic<uint64_t> actual_processor_executions_{0};

public:
    void recordMetrics(const PaymentResponse& res) {
        total_requests_.fetch_add(1, std::memory_order_relaxed);

        if (res.is_duplicate) {
            duplicate_requests_count_.fetch_add(1, std::memory_order_relaxed);
        }

        if (res.status == TransactionState::SUCCESS) {
            success_count_.fetch_add(1, std::memory_order_relaxed);
        } else if (res.status == TransactionState::FAILED) {
            failure_count_.fetch_add(1, std::memory_order_relaxed);
        }

        std::lock_guard<std::mutex> lock(mutex_);
        queue_latencies_ns_.push_back(res.queue_latency_ns);
        processing_latencies_ns_.push_back(res.processing_latency_ns);
        e2e_latencies_ns_.push_back(res.total_e2e_latency_ns);
    }

    void incrementActualProcessorExecutions() {
        actual_processor_executions_.fetch_add(1, std::memory_order_relaxed);
    }

    static uint64_t getPercentile(const std::vector<uint64_t>& sorted, double p) {
        if (sorted.empty()) return 0;
        size_t idx = static_cast<size_t>(sorted.size() * p);
        if (idx >= sorted.size()) idx = sorted.size() - 1;
        return sorted[idx];
    }

    void printReport(double total_duration_sec) {
        std::lock_guard<std::mutex> lock(mutex_);
        
        std::sort(queue_latencies_ns_.begin(), queue_latencies_ns_.end());
        std::sort(processing_latencies_ns_.begin(), processing_latencies_ns_.end());
        std::sort(e2e_latencies_ns_.begin(), e2e_latencies_ns_.end());

        std::cout << "\n========================================================\n";
        std::cout << "          PAYMENT ENGINE PERFORMANCE REPORT             \n";
        std::cout << "========================================================\n";
        std::cout << " Total Incoming Requests   : " << total_requests_.load() << "\n";
        std::cout << " Successful Transactions   : " << success_count_.load() << "\n";
        std::cout << " Failed Transactions       : " << failure_count_.load() << "\n";
        std::cout << " Duplicate Requests Caught : " << duplicate_requests_count_.load() << "\n";
        std::cout << " Actual Processor Hits     : " << actual_processor_executions_.load() << "\n";
        std::cout << " Total Time Elapsed        : " << std::fixed << std::setprecision(4) << total_duration_sec << " s\n";
        std::cout << " Throughput                : " << static_cast<size_t>(total_requests_.load() / total_duration_sec) << " req/sec\n";
        std::cout << "--------------------------------------------------------\n";
        std::cout << " LATENCY PROFILE (Nanoseconds / Microseconds):\n";
        std::cout << " Metric      | Queue Latency     | Processing Latency| End-to-End Latency\n";
        std::cout << "-------------+-------------------+-------------------+-------------------\n";

        auto printRow = [](const std::string& label, uint64_t q, uint64_t p, uint64_t e2e) {
            std::cout << " " << std::left << std::setw(11) << label << " | "
                      << std::right << std::setw(8) << q << " ns (" << std::setw(4) << q/1000 << "us) | "
                      << std::right << std::setw(8) << p << " ns (" << std::setw(4) << p/1000 << "us) | "
                      << std::right << std::setw(8) << e2e << " ns (" << std::setw(4) << e2e/1000 << "us)\n";
        };

        printRow("P50", getPercentile(queue_latencies_ns_, 0.50), getPercentile(processing_latencies_ns_, 0.50), getPercentile(e2e_latencies_ns_, 0.50));
        printRow("P90", getPercentile(queue_latencies_ns_, 0.90), getPercentile(processing_latencies_ns_, 0.90), getPercentile(e2e_latencies_ns_, 0.90));
        printRow("P95", getPercentile(queue_latencies_ns_, 0.95), getPercentile(processing_latencies_ns_, 0.95), getPercentile(e2e_latencies_ns_, 0.95));
        printRow("P99", getPercentile(queue_latencies_ns_, 0.99), getPercentile(processing_latencies_ns_, 0.99), getPercentile(e2e_latencies_ns_, 0.99));
        printRow("P99.9", getPercentile(queue_latencies_ns_, 0.999), getPercentile(processing_latencies_ns_, 0.999), getPercentile(e2e_latencies_ns_, 0.999));
        std::cout << "========================================================\n\n";
    }
};

// ============================================================================
// 7. THREAD POOL
// ============================================================================

class ThreadPool {
private:
    std::vector<std::thread> workers_;
    std::queue<std::function<void()>> tasks_;
    std::mutex queue_mutex_;
    std::condition_variable cv_;
    std::atomic<bool> stop_{false};

public:
    explicit ThreadPool(size_t threads) {
        for (size_t i = 0; i < threads; ++i) {
            workers_.emplace_back([this]() {
                while (true) {
                    std::function<void()> task;
                    {
                        std::unique_lock<std::mutex> lock(this->queue_mutex_);
                        this->cv_.wait(lock, [this]() {
                            return this->stop_ || !this->tasks_.empty();
                        });
                        if (this->stop_ && this->tasks_.empty()) return;
                        task = std::move(this->tasks_.front());
                        this->tasks_.pop();
                    }
                    task();
                }
            });
        }
    }

    template <class F>
    auto enqueue(F&& f) -> std::future<typename std::invoke_result<F>::type> {
        using return_type = typename std::invoke_result<F>::type;

        auto task = std::make_shared<std::packaged_task<return_type()>>(std::forward<F>(f));
        std::future<return_type> res = task->get_future();

        {
            std::lock_guard<std::mutex> lock(queue_mutex_);
            if (stop_) throw std::runtime_error("Enqueue requested on stopped ThreadPool");
            tasks_.emplace([task]() { (*task)(); });
        }
        cv_.notify_one();
        return res;
    }

    ~ThreadPool() {
        stop_ = true;
        cv_.notify_all();
        for (std::thread& worker : workers_) {
            if (worker.joinable()) worker.join();
        }
    }
};

// ============================================================================
// 8. CORE ENGINE
// ============================================================================

class PaymentGatewayEngine {
private:
    ThreadPool pool_;
    AtomicIdempotencyManager idempotency_mgr_;
    ProcessorRegistry processor_registry_;
    MetricsMonitor metrics_;
    MySQLPersistenceManager db_manager_;

public:
    explicit PaymentGatewayEngine(size_t worker_count) 
        : pool_(worker_count),
          db_manager_("127.0.0.1", "root", "Ashu@1234", "payment_gateway_db") {}

    std::future<PaymentResponse> processTransactionAsync(PaymentRequest request) {
        request.ingress_timestamp = std::chrono::high_resolution_clock::now();

        return pool_.enqueue([this, req = std::move(request)]() mutable -> PaymentResponse {
            auto worker_pickup_timestamp = std::chrono::high_resolution_clock::now();
            
            PaymentResponse response;
            response.transaction_id = req.transaction_id;
            response.idempotency_key = req.idempotency_key;

            response.queue_latency_ns = std::chrono::duration_cast<std::chrono::nanoseconds>(
                worker_pickup_timestamp - req.ingress_timestamp
            ).count();

            std::shared_ptr<AtomicIdempotencyManager::Entry> reservation_entry;
            ReservationStatus status = idempotency_mgr_.getOrReserve(req.idempotency_key, response, reservation_entry);

            if (status == ReservationStatus::COMPLETED || status == ReservationStatus::PENDING_FOLLOWER) {
                if (status == ReservationStatus::PENDING_FOLLOWER) {
                    idempotency_mgr_.waitAndCopyResult(reservation_entry, response, req.idempotency_key);
                }
                auto completion_timestamp = std::chrono::high_resolution_clock::now();
                response.total_e2e_latency_ns = std::chrono::duration_cast<std::chrono::nanoseconds>(
                    completion_timestamp - req.ingress_timestamp
                ).count();
                metrics_.recordMetrics(response);
                return response;
            }

            // LEADER THREAD EXECUTION
            auto proc_start_timestamp = std::chrono::high_resolution_clock::now();

            TransactionState state = TransactionState::CREATED;
            TransactionStateMachine::transitionTo(state, TransactionState::VALIDATING);

            if (req.amount <= 0 || req.merchant_id.empty()) {
                TransactionStateMachine::transitionTo(state, TransactionState::FAILED);
                response.status = state;
                response.response_code = "ERR_VALIDATION";
                response.message = "Validation check failed";
            } else {
                TransactionStateMachine::transitionTo(state, TransactionState::PROCESSING);

                metrics_.incrementActualProcessorExecutions();
                const PaymentProcessor& processor = processor_registry_.getProcessor(req.payment_method);
                processor.process(req, response);

                if (response.status == TransactionState::AUTHORIZED) {
                    TransactionStateMachine::transitionTo(response.status, TransactionState::CAPTURED);
                    TransactionStateMachine::transitionTo(response.status, TransactionState::SUCCESS);
                } else {
                    TransactionStateMachine::transitionTo(response.status, TransactionState::FAILED);
                }
            }

            auto proc_end_timestamp = std::chrono::high_resolution_clock::now();
            
            response.processing_latency_ns = std::chrono::duration_cast<std::chrono::nanoseconds>(
                proc_end_timestamp - proc_start_timestamp
            ).count();
            
            response.total_e2e_latency_ns = std::chrono::duration_cast<std::chrono::nanoseconds>(
                proc_end_timestamp - req.ingress_timestamp
            ).count();

            idempotency_mgr_.commitResult(req.idempotency_key, reservation_entry, response);
            metrics_.recordMetrics(response);

            // Async Database Persistence
            db_manager_.asyncPersist({
                req.transaction_id,
                req.idempotency_key,
                req.merchant_id,
                req.amount,
                req.currency,
                paymentMethodToString(req.payment_method),
                transactionStateToString(response.status),
                response.response_code
            });

            return response;
        });
    }

    void printMetricsReport(double duration_sec) {
        metrics_.printReport(duration_sec);
    }
};

// ============================================================================
// 9. BENCHMARK MAIN
// ============================================================================

int main() {
    constexpr size_t THREAD_POOL_SIZE = 8;
    constexpr size_t TOTAL_TRANSACTIONS = 10000;

    std::cout << "========================================================\n";
    std::cout << " STARTING PAYMENT ENGINE (Arch Linux Native API)        \n";
    std::cout << "========================================================\n";

    PaymentGatewayEngine engine(THREAD_POOL_SIZE);

    std::vector<std::future<PaymentResponse>> futures;
    futures.reserve(TOTAL_TRANSACTIONS);

    auto start_time = std::chrono::high_resolution_clock::now();

    for (size_t i = 0; i < TOTAL_TRANSACTIONS; ++i) {
        PaymentRequest req;
        req.transaction_id = "TXN_" + std::to_string(i);
        req.idempotency_key = "IDEMP_" + std::to_string(i % 8000);
        req.merchant_id = "MERCHANT_PROD";
        req.amount = 100 + (i % 1000);
        req.currency = "USD";
        req.payment_method = static_cast<PaymentMethod>(i % 4);
        req.account_details = "4111-XXXX-XXXX-1111";

        futures.push_back(engine.processTransactionAsync(req));
    }

    for (auto& fut : futures) {
        fut.wait();
    }

    auto end_time = std::chrono::high_resolution_clock::now();
    double duration_sec = std::chrono::duration<double>(end_time - start_time).count();

    engine.printMetricsReport(duration_sec);

    return 0;
}