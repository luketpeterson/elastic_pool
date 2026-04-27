# Architecture Plan: Elixir-Prolog Bridge

## Objective
Create a high-performance communication bridge between Elixir and SWI-Prolog using Erlang Ports. The system maintains a pool of persistent SWI-Prolog processes to avoid reload latency for large knowledge bases.

## System Architecture

The system uses a custom reactive scaling architecture to handle high-throughput loads while maintaining a stable baseline of workers.

### 1. `PrologBridge` (High-level API)
*   Provides the primary `query/2` interface.
*   Coordinates with `PrologBridge.Pool` for worker checkouts and triggers scaling if the pool is exhausted.

### 2. `PrologBridge.Pool` (Queue-based Pool)
*   **Role:** Manages ready-to-use worker PIDs in an internal queue.
*   **Mechanism:** Uses a GenServer to track available workers and a waiting queue for clients. It monitors workers and handles clean-up if a Prolog process crashes.

### 3. `PrologBridge.ScalingManager` (Gatekeeper)
*   **Role:** Governs the growth of the worker pool.
*   **Logic:** Implements a "sane scaling" policy using:
    *   **Cooldown:** A mandatory wait period (e.g., 500ms) between scale-up events.
    *   **Threshold:** Only scales up if the internal waiting queue exceeds a configurable limit (e.g., 100 clients).
    *   **Locking:** Ensures only one worker is in the "spinning up" state at a time.

### 4. `PrologBridge.Worker` (GenServer)
*   **Role:** Manages a single `swipl` OS process via Erlang Ports.
*   **Initialization:** Uses `handle_continue` for non-blocking startup, allowing multiple workers to spin up in parallel during baseline initialization.
*   **Protocol:** Communicates using NDJSON (Newline Delimited JSON).

### 5. `PrologBridge.WorkerSupervisor` (DynamicSupervisor)
*   **Role:** Responsible for the lifecycle of worker processes, allowing the `ScalingManager` to spawn new instances dynamically.

### 6. `PrologBridge.Application`
*   **Role:** Centralizes system configuration (max workers, baseline, cooldown, threshold) and manages the supervision tree.
*   **Baseline:** Starts the initial set of parallel workers on boot via `PrologBridge.start_baseline/1`.

## Communication Protocol
*   **Request:** `{"query": "ancestor(X, Y)."}`
*   **Response:** `{"status": "success", "results": [...]}`
*   **Transport:** Standard I/O via Erlang Ports.

## Prolog Side: `server.pl`
*   A persistent SWI-Prolog process that loads the KB once and enters a JSON-based Read-Eval-Print Loop (REPL).
