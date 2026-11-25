package it.unitn.ds.tui;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * A scripted course of events, run on a fresh system. Each step is a TUI action ({@code read <client> <replica>
 * <idx>}, {@code write <client> <replica> <idx> <val>}, {@code crash <replica> [type] [n]}), {@code wait <ms>} or
 * {@code say <text>} (narration).
 */
record Scenario(String description, int replicas, int clients, int coordinator, List<String> steps) {

    static final Map<String, Scenario> ALL = new LinkedHashMap<>();

    static final String LEGEND = "● client sends  ► replica receives  ✗ crash  ⚑ scheduled crash  ┆ wait";
    private static final int LANE = 4, WRAP = 60;

    String setup() {
        return replicas + " replicas (coordinator R" + coordinator + "), " + clients + " clients";
    }

    /** Lane titles: one column per client, then one per replica. */
    String lanes() {
        StringBuilder b = new StringBuilder();
        for (int l = 0; l < clients + replicas; l++)
            b.append(String.format("%-" + LANE + "s", l < clients ? "C" + l : "R" + (l - clients)));
        return b.toString();
    }

    /**
     * One entry per step, aligned under {@link #lanes()}: a sequence-diagram row for actions, wrapped text for
     * narration. Entries may span several lines.
     */
    List<String> diagram() {
        int lanes = clients + replicas;
        boolean[] crashed = new boolean[lanes];
        List<String> out = new ArrayList<>();
        for (String step : steps) {
            String[] a = step.split(" ");
            if (a[0].equals("say")) {
                out.add(wrap("» " + step.substring(4)));
                continue;
            }
            char[] row = new char[lanes * LANE];
            Arrays.fill(row, ' ');
            for (int l = 0; l < lanes; l++) if (!crashed[l]) row[l * LANE] = a[0].equals("wait") ? '┆' : '│';
            String label =
                    switch (a[0]) {
                        case "wait" -> String.format("wait %.1f s", Integer.parseInt(a[1]) / 1000.0);
                        case "read", "write" -> {
                            int from = Integer.parseInt(a[1]) * LANE, to = (clients + Integer.parseInt(a[2])) * LANE;
                            Arrays.fill(row, from, to, '─');
                            row[from] = '●';
                            row[to] = '►';
                            yield a[0].equals("read") ? "read [" + a[3] + "]" : "write [" + a[3] + "]=" + a[4];
                        }
                        case "crash" -> {
                            int lane = clients + Integer.parseInt(a[1]);
                            if (a.length < 3 || a[2].equals("Now")) {
                                row[lane * LANE] = '✗';
                                crashed[lane] = true;
                                yield "R" + a[1] + " crashes";
                            }
                            row[lane * LANE] = '⚑';
                            yield "R" + a[1] + " will crash after " + a[3] + " " + a[2] + " message(s)";
                        }
                        default -> step;
                    };
            out.add(new String(row) + label);
        }
        return out;
    }

    private static String wrap(String text) {
        StringBuilder b = new StringBuilder();
        int lineStart = 0;
        for (String word : text.split(" ")) {
            if (b.length() - lineStart + word.length() > WRAP) {
                b.append("\n  ");
                lineStart = b.length() - 2;
            } else if (!b.isEmpty()) b.append(' ');
            b.append(word);
        }
        return b.toString();
    }

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
