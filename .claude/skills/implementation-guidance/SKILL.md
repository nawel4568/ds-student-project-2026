---
name: implementation-guidance
description: Use when the user asks how to implement a feature of the replica/client protocol (heartbeat, election, crash handling, timeouts, recovery, ...) and wants high-level guidance or brainstorming, not a solution. Produces a design walkthrough grounded in this repo's code and tests, without writing the implementation.
---

# High-level implementation guidance

The user writes the code; you give the map. Do not edit `src/` and do not
paste a working implementation. Short illustrative fragments (a message
class name, a signature, an Akka API name) are fine; full handlers are not.

## 1. Ground yourself first

Read before answering, never guess:

- `README.md`: mandatory API, callbacks, timing rules.
- `AbstractReplica.java` / `AbstractClient.java`: javadoc is the contract
  (callback semantics, `Crash` semantics, latency getters).
- `Replica.java` / `Client.java`: the user's current state. Note fields,
  helpers and TODOs that already exist; build on them, flag what is
  half-wired (e.g. state set but never read).
- `src/test/java/.../TestsCommons.java`: the timing budgets the tests
  allow (update delay, election delay, client timeouts). Derive any
  timeout bound from these formulas and quote them.
- The tests in `src/test/java/.../base/` that exercise the feature.

## 2. Answer shape

Use this order, terse, one bullet per fact or decision:

1. **Goal**: what the feature must achieve and which README/javadoc rule
   or test requires it.
2. **Roles**: who does what (coordinator vs replica vs client), and when
   the role starts and stops (init, after election, on crash).
3. **Pieces to add**: messages (and what they carry), state fields,
   timers, handlers. Names only, no bodies.
4. **Design choices**: for each real fork, 2-3 options with one-line
   tradeoffs and a recommendation.
5. **Interactions with existing code**: crash emulation, the single send
   path (`AbstractReplica.tell` for emulated latency), mandatory
   callbacks, other protocol phases.
6. **Pitfalls**: timing bounds, stale/duplicate messages, ordering,
   things that make tests flaky.
7. **How to verify**: which test, or which observable log/callback.

## 3. Repo-specific reminders

- Actors are single-threaded: timers must deliver a message to `self`,
  never touch state from a scheduler callback.
- All protocol traffic goes through `AbstractReplica.tell`; direct
  `ActorRef.tell` skips the emulated latency the tests rely on.
- `NetworkChannel` is FIFO per (sender, destination) and delays each
  queued message in turn, so a message behind a burst arrives late.
- Use `debug()` for chatty logs; `log()` and stdout slow the tests.
- Crash types count *processed* messages; say which message a new
  feature contributes to which `Crash.Type`.

## 4. Afterwards

Per `AGENTS.md`, update `report/99_llm_tools.tex` with the purpose of the
help if it is not already listed. Do not touch other report sections
unless asked.
