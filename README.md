

# C++ High-Speed Payment Processing Engine

A fast, reliable payment engine built in **C++17**. It handles thousands of payment requests per second, prevents double-charging customers, and saves transactions to a database without slowing down the system.

During testing, it processed **10,000 transactions in under 0.1 seconds (~144,000 transactions per second)** with an internal execution time of just **336 nanoseconds** per payment.

---

## 💡 What Problem Does This Solve?

When millions of people buy things online at the same time, two main problems happen:
1. **Slow Checkout:** Databases get overwhelmed, making users wait.
2. **Double Charging:** If a user clicks "Pay" twice because their internet lagged, they might get charged twice.

---

## 🛠️ How It Works (In Simple Terms)

Think of this engine like a high-speed bank drive-thru:

1. **The Worker Pool (8 Workers):** Instead of one clerk handling all customers, 8 dedicated workers process payments at the exact same time.
2. **The Double-Charge Guard (Idempotency):** Before charging a card, the engine checks if the payment request was already sent. If a user clicks "Pay" twice, the second request is caught instantly and handed the copy of the first receipt.
3. **The Rules Enforcer (State Machine):** A payment must strictly follow steps in order (`Created` ➔ `Validated` ➔ `Authorized` ➔ `Completed`). It prevents weird errors like refunding a payment that was never made.
4. **The Background Record Keeper (Async Database):** Instead of forcing the customer to wait while writing data to disk, the engine approves the payment instantly and hands the transaction receipt to a background clerk to save in MySQL later.

---

## ⚡ Performance Numbers

Tested on **Linux (Arch Linux)** with 10,000 simultaneous requests:

| What Was Tested | Result | What It Means |
| :--- | :--- | :--- |
| **Total Speed** | **144,545 req/sec** | Can handle huge traffic spikes without crashing. |
| **Duplicate Prevention** | **2,000 caught** | Exactly 2,000 duplicate requests were blocked instantly. |
| **Core Payment Time (P50)**| **336 nanoseconds** | Takes less than 1 microsecond to process the logic. |
| **Total Time for 10k Items**| **0.069 seconds** | Processed 10,000 payments faster than a blink of an eye. |

---

## 🚀 How to Run It

### Prerequisites
* A C++ compiler (`g++`)
* MariaDB / MySQL client library installed (`mariadb-libs` on Arch, `libmariadb-dev` on Ubuntu)

### 1. Compile
```bash
g++ -std=c++17 -O3 -pthread main.cpp -lmariadb -o payment_engine

2. Run
Bash

./payment_engine

📜 License

This project is open-source under the MIT License.
