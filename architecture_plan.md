# Architecture Plan: Elixir-Prolog Bridge Proof of Concept

## Objective
Create a Proof of Concept (PoC) demonstrating a persistent, low-latency communication bridge between Elixir and SWI-Prolog using Erlang Ports. The SWI-Prolog process will be long-running to keep a large knowledge base (KB) in memory, avoiding reload latency for individual queries.

## Proposed Architecture

The system will rely on a **Worker Pool** managed by `poolboy`. This allows Elixir to maintain a minimum number of persistent Prolog processes while scaling up to handle bursts.

### 1. Elixir Side: `PrologBridge.Worker` (GenServer)
*   **Role:** Manages a single `swipl` OS process via Erlang Ports.
*   **Scaling:** Managed by `poolboy`.
    *   **Base Size:** 2 (starts 2 processes on boot).
    *   **Max Overflow:** 10 (spins up additional processes as load increases).

### 2. Communication Protocol: NDJSON
*   **Request:** `{"query": "ancestor(X, Y).", "id": "uuid"}`
*   **Response:** `{"status": "success", "results": [...], "id": "uuid"}`

### 3. Prolog Side: `server.pl`
*   **Role:** Persistent SWI-Prolog process that loads the KB once and enters a JSON-based Read-Eval-Print Loop (REPL).

## Implementation Steps

1.  **Initialize Project:**
    *   `mix new . --module PrologBridge --sup`
    *   Add `:jason` and `:poolboy` to `mix.exs`.
2.  **Prolog Implementation:**
    *   `kb.pl`: The Knowledge Base.
    *   `server.pl`: The JSON interface for `swipl`.
3.  **Elixir Implementation:**
    *   `PrologBridge.Worker`: The port-handling GenServer.
    *   `PrologBridge.Application`: Configure the `poolboy` child spec.
    *   `PrologBridge`: High-level API to "checkout" workers and run queries.

## Future Considerations (Beyond PoC)
*   **Concurrency:** Erlang Ports are single-threaded from the Elixir GenServer's perspective. If high throughput is needed later, a pool of GenServers (using `NimblePool` or `Poolboy`) managing multiple Prolog instances can be implemented.
*   **Prolog Safety:** Implementing timeouts and sandboxing in Prolog to prevent runaway queries from blocking the Port indefinitely.
