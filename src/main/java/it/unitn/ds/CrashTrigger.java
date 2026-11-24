package it.unitn.ds;

import it.unitn.ds.AbstractReplica.Crash;

/**
 * Deferred crash: "crash after processing n messages of a given type". Plain object owned by one actor, so no
 * synchronisation is needed.
 */
final class CrashTrigger {
    private Crash.Type type; // null = not armed
    private int budget;

    void arm(Crash how) {
        this.type = how.type;
        this.budget = how.after_n_messages_of_type;
    }

    /**
     * Counts one processed message of type {@code t}. Call it after the message is processed, or inside a loop (e.g. a
     * broadcast) to crash part-way through.
     *
     * @return true if the replica must crash now
     */
    boolean tick(Crash.Type t) {
        if (t != type || --budget > 0) return false;
        type = null;
        return true;
    }
}
