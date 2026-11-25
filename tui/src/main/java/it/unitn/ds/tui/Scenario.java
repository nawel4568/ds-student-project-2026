package it.unitn.ds.tui;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * A scripted course of events, run on a fresh system. Each step is a TUI command, {@code wait <ms>} or
 * {@code say <text>} (narration shown above the prompt).
 */
record Scenario(String description, int replicas, int clients, int coordinator, List<String> steps) {

    static final Map<String, Scenario> ALL = new LinkedHashMap<>();

    static {
        ALL.put(
                "concurrent-writes",
                new Scenario(
                        "two clients write the same index through different replicas: all replicas agree on the order",
                        5,
                        2,
                        0,
                        List.of(
                                "say C0 writes [1]=10 via R1 and C1 writes [1]=20 via R4, at the same time",
                                "write 0 1 1 10",
                                "write 1 4 1 20",
                                "wait 2000",
                                "say Every replica holds the same final value for [1]",
                                "read 0 2 1",
                                "read 1 3 1")));
        ALL.put(
                "coordinator-crash",
                new Scenario(
                        "coordinator crashes: heartbeat timeout, ring election, writes resume",
                        5,
                        2,
                        0,
                        List.of(
                                "say Write [1]=10 and let it commit everywhere",
                                "write 0 1 1 10",
                                "wait 1500",
                                "say Crash the coordinator R0",
                                "crash 0",
                                "say Replicas miss R0's heartbeat, then elect a new coordinator",
                                "wait 4000",
                                "say Write [2]=20 via R3: served by the new coordinator",
                                "write 0 3 2 20",
                                "wait 1500",
                                "read 1 4 2",
                                "wait 1000",
                                "say Done: the new coordinator committed [2]=20")));
        ALL.put(
                "crash-mid-update",
                new Scenario(
                        "coordinator crashes after sending UPDATE to R1 only: the election completes the update",
                        5,
                        2,
                        0,
                        List.of(
                                "say Arm R0 to crash after sending 1 UPDATE message",
                                "crash 0 Update 1",
                                "say Write [1]=10 via R2: only R1 receives the UPDATE",
                                "write 0 2 1 10",
                                "say R2 times out on its forwarded write, an election starts; R1 has the newest update",
                                "wait 5000",
                                "say The new coordinator re-sends the pending update: all live replicas apply [1]=10",
                                "read 1 3 1")));
        ALL.put(
                "crash-mid-writeok",
                new Scenario(
                        "coordinator crashes after sending WRITEOK to R1 only: the others get it after the election",
                        5,
                        2,
                        0,
                        List.of(
                                "say Arm R0 to crash after sending 1 WRITEOK message",
                                "crash 0 WriteOK 1",
                                "say Write [1]=10 via R3: only R0 and R1 apply it",
                                "write 0 3 1 10",
                                "say R2..R4 time out waiting for WRITEOK and start an election",
                                "wait 5000",
                                "say The new coordinator delivers the update: no replica misses [1]=10",
                                "read 1 4 1")));
        ALL.put(
                "heartbeat-crash",
                new Scenario(
                        "coordinator crashes half-way through a heartbeat broadcast",
                        5,
                        2,
                        0,
                        List.of(
                                "say Arm R0 to crash after sending 6 HEARTBEATs (4 per beat: mid second beat)",
                                "crash 0 Heartbeat 6",
                                "say Replicas that missed the beat detect the crash first and start the election",
                                "wait 6000",
                                "write 0 2 1 10",
                                "wait 1500",
                                "say Done: the system is writable again")));
        ALL.put(
                "crash-during-election",
                new Scenario(
                        "a replica crashes while the election token circulates: the ring skips it",
                        5,
                        2,
                        0,
                        List.of(
                                "say Arm R1 (R0's successor) to crash after its first ELECTION message",
                                "crash 1 Election 1",
                                "say Crash the coordinator R0",
                                "crash 0",
                                "say R1 crashes mid-election; senders time out on its ELECTIONACK and skip it",
                                "wait 7000",
                                "write 0 3 1 10",
                                "wait 1500",
                                "say Done: elected with two crashed replicas")));
        ALL.put(
                "replica-crashes",
                new Scenario(
                        "non-coordinator crashes: writes commit as long as a quorum is alive",
                        5,
                        2,
                        0,
                        List.of(
                                "say Crash R3 and R4: 3 of 5 replicas alive, still a quorum",
                                "crash 3",
                                "crash 4",
                                "write 0 1 1 10",
                                "wait 1500",
                                "say Reading from a crashed replica: the client times out",
                                "read 1 4 1",
                                "wait 2500",
                                "say Done")));
    }
}
