package it.unitn.ds;

import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.Serializable;
import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Stream;

import org.junit.jupiter.api.Test;

import akka.actor.ActorRef;

/**
 * Spec: "Shared mutable state is forbidden; any shared objects must be immutable."
 * Akka passes local messages by reference, so every message class we define (and every
 * object reachable from one) must be deeply immutable.
 */
class ImmutableMessagesTest {

    // Types that are immutable by contract. ActorRef is an immutable handle to an actor.
    private static final Set<Class<?>> IMMUTABLE_LEAVES = Set.of(
            String.class, Integer.class, Long.class, Short.class, Byte.class, Character.class,
            Boolean.class, Double.class, Float.class, ActorRef.class);

    @Test
    void messagesAreDeeplyImmutable() {
        List<String> violations = new ArrayList<>();
        Set<Class<?>> visited = new HashSet<>();
        Stream.of(Messages.class, Replica.class, Client.class)
                .flatMap(c -> Stream.of(c.getDeclaredClasses()))
                .filter(Serializable.class::isAssignableFrom)
                .forEach(c -> check(c, c.getSimpleName(), visited, violations));

        assertTrue(violations.isEmpty(), "Mutable state reachable from messages:\n  "
                + String.join("\n  ", violations));
    }

    private static void check(Class<?> type, String path, Set<Class<?>> visited, List<String> violations) {
        if (type.isPrimitive() || type.isEnum() || IMMUTABLE_LEAVES.contains(type) || !visited.add(type))
            return;
        if (!type.getPackageName().equals(Messages.class.getPackageName())) {
            violations.add(path + ": type " + type.getName() + " is not known to be immutable");
            return;
        }
        for (Class<?> c = type; c != null && c != Object.class; c = c.getSuperclass()) {
            for (Field f : c.getDeclaredFields()) {
                if (f.isSynthetic())
                    continue;
                String fieldPath = path + "." + f.getName();
                if (!Modifier.isFinal(f.getModifiers()))
                    violations.add(fieldPath + ": field is not final");
                if (!Modifier.isStatic(f.getModifiers()))
                    check(f.getType(), fieldPath, visited, violations);
            }
        }
    }
}
