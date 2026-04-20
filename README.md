# Networked and Distributed Systems (NSDS) - A.Y. 2025/2026

![PoliMi](https://img.shields.io/badge/University-Politecnico%20di%20Milano-blue)
![Grade](https://img.shields.io/badge/Grade-30%2F30-brightgreen)
![Course](https://img.shields.io/badge/Course-Computer%20Science%20Engineering-orange)

This repository contains the complete set of laboratories and projects for the **Networked and Distributed Systems (NSDS)** course at Politecnico di Milano. The course focuses on the architectural and implementation aspects of modern distributed computing.

## 📂 Repository Structure

The project is organized into specific modules, each covering a different technology or paradigm:

* **`akka/`**: Distributed systems using the Actor model (Java). Implementation of fault-tolerant communication.
* **`kafka/`**: Real-time data streaming. Management of producers, consumers, and stream merging logic.
* **`spark/`**: Big Data processing and analytics using Spark SQL and RDDs.
* **`mpi/`**: Parallel computing using the Message Passing Interface.
* **`nodered/`**: Flow-based programming for IoT and event-driven applications.
* **`iot/`**: Exercises and examples focused on Internet of Things protocols and architectures.

---

## 📝 Lab Evaluations & Lessons Learned

While the final grade was **30/30**, some feedback from the professors (Prof. Mottola and Prof. Margara) highlighted critical edge cases. I've documented them here to provide a better learning resource:

### 🔹 Akka - Fault Tolerance
> **Feedback:** *"You are assuming that no matter what goes wrong at the Client, a TimeoutMsg was received instead of a ReplyMsg..."*
* **Insight:** In distributed systems, you cannot guarantee the type of failure message. Ensure your actor logic is robust enough to handle unexpected message types or the total absence of a response, rather than relying solely on a specific `TimeoutMsg`.

### 🔹 Kafka - Stream Merging
> **Feedback:** *"Merger should wait for the send to be acknowledged before committing the offsets."*
* **Insight:** To guarantee **at-least-once** or **exactly-once** semantics, always await the acknowledgment (ACK) from the Kafka broker before committing the consumer offsets. Committing too early can lead to data loss if the producer fails immediately after.

### 🔹 Spark - Performance Optimization
> **Feedback:** *"Cache after show (should be before)."*
* **Insight:** Spark's `.cache()` is lazy. It must be called **before** the first action (like `show()`, `count()`, or `collect()`) that triggers the computation. Caching *after* an action means the first execution doesn't benefit from memory storage, wasting computation time.

---

## 🚀 How to use this repo
1.  **Reference:** Use the code to understand the implementation patterns for each technology.
2.  **Edge Cases:** Pay attention to the "Lessons Learned" section to avoid common pitfalls during the oral exam or lab evaluation.
3.  **Setup:** Each folder contains the necessary source files to run the examples in their respective environments.

## 👤 Author
* **Matteo Galimberti** - [GitHub Profile](https://github.com/Galimba03)
* **Davide Ghisolfi** - [GitHub Profile](https://github.com/DavideGhiiso)
* **Luca Komisarjevsky** - [GitHub Profile](https://github.com/LucaKomi)

---
*Disclaimer: This repository is intended for personal study and research purposes only. Please adhere to the Politecnico di Milano academic integrity policies.*