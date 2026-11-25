package it.unitn.ds.tui;

import akka.actor.AbstractActor;
import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.actor.Props;
import com.googlecode.lanterna.TerminalSize;
import com.googlecode.lanterna.TextColor;
import com.googlecode.lanterna.graphics.SimpleTheme;
import com.googlecode.lanterna.gui2.BasicWindow;
import com.googlecode.lanterna.gui2.BorderLayout;
import com.googlecode.lanterna.gui2.Borders;
import com.googlecode.lanterna.gui2.Direction;
import com.googlecode.lanterna.gui2.Label;
import com.googlecode.lanterna.gui2.LinearLayout;
import com.googlecode.lanterna.gui2.MultiWindowTextGUI;
import com.googlecode.lanterna.gui2.Panel;
import com.googlecode.lanterna.gui2.TextBox;
import com.googlecode.lanterna.gui2.TextGUIGraphics;
import com.googlecode.lanterna.gui2.Window;
import com.googlecode.lanterna.gui2.table.DefaultTableCellRenderer;
import com.googlecode.lanterna.gui2.table.Table;
import com.googlecode.lanterna.gui2.table.TableModel;
import com.googlecode.lanterna.input.KeyStroke;
import com.googlecode.lanterna.screen.Screen;
import com.googlecode.lanterna.terminal.DefaultTerminalFactory;
import com.typesafe.config.ConfigFactory;
import it.unitn.ds.AbstractClient.ReadRequest;
import it.unitn.ds.AbstractClient.ReadResult;
import it.unitn.ds.AbstractClient.ReadTimeout;
import it.unitn.ds.AbstractClient.WriteRequest;
import it.unitn.ds.AbstractClient.WriteResult;
import it.unitn.ds.AbstractClient.WriteTimeout;
import it.unitn.ds.AbstractReplica;
import it.unitn.ds.AbstractReplica.CoordinatorElected;
import it.unitn.ds.AbstractReplica.Crash;
import it.unitn.ds.AbstractReplica.ElectionStarted;
import it.unitn.ds.AbstractReplica.InitSystem;
import it.unitn.ds.AbstractReplica.UpdateApplied;
import it.unitn.ds.Client;
import it.unitn.ds.Logger;
import it.unitn.ds.Replica;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

/**
 * Demo console for the replicated system. It only uses the public API (props, InitSystem, Crash,
 * Read/WriteRequest) and renders what the API callbacks and the Logger report.
 *
 * <p>Usage: {@code tui [replicas=5] [clients=3] [coordinator=0]}
 */
public final class Tui {
    private static final long READ_TIMEOUT_MS = 2000;
    // Must outlast heartbeat detection + election, otherwise writes during an election time out.
    private static final long WRITE_TIMEOUT_MS = 10000;
    private static final int LOG_LINES_KEPT = 500;

    /** Command grammar: drives execution help, the argument hint and Tab completion. */
    private static final Map<String, List<String>> USAGE = new LinkedHashMap<>();

    static {
        USAGE.put("read", List.of("<client>", "<replica>", "<idx>"));
        USAGE.put("write", List.of("<client>", "<replica>", "<idx>", "<val>"));
        USAGE.put("crash", List.of("<replica>", "[type]", "[n]"));
        USAGE.put("scenario", List.of("[scenario]"));
        USAGE.put("reset", List.of());
        USAGE.put("debug", List.of());
        USAGE.put("quit", List.of());
    }

    private static final Map<String, String> ALIASES = Map.of("r", "read", "w", "write", "c", "crash", "q", "quit");
    private static final String HELP = USAGE.entrySet().stream()
            .map(e -> String.join(" ", e.getKey(), String.join(" ", e.getValue())).trim())
            .collect(Collectors.joining(" | "));

    record Event(ActorRef from, Object msg) {}

    /** Listener given to every replica and client: forwards each callback to the UI thread. */
    static final class Probe extends AbstractActor {
        private final Queue<Event> out;

        Probe(Queue<Event> out) {
            this.out = out;
        }

        @Override
        public Receive createReceive() {
            return receiveBuilder().matchAny(m -> out.add(new Event(getSender(), m))).build();
        }
    }

    static final class ReplicaView {
        boolean crashed, electing;
        String armed = ""; // pending deferred crash, as requested via the API
        int coordinator;
        final Map<Integer, Integer> values = new TreeMap<>();
    }

    static final class ClientView {
        int pending;
        String last = "-";
    }

    private final int nReplicas, nClients, coordinator; // command-line setup, restored by "reset"
    private ActorSystem system;
    private Queue<Event> events;
    private final List<ActorRef> replicas = new ArrayList<>();
    private final List<ActorRef> clients = new ArrayList<>();
    private final Map<ActorRef, Integer> replicaIds = new HashMap<>();
    private final Map<ActorRef, Integer> clientIds = new HashMap<>();
    private ReplicaView[] replicaViews;
    private ClientView[] clientViews;
    private final Deque<String> log = new ArrayDeque<>();
    private final RandomAccessFile logFile;
    private boolean debug;

    // Running scenario: remaining steps and when the next one is due
    private final Deque<String> script = new ArrayDeque<>();
    private long nextStepAt;
    private String scenarioName = "";

    // Widgets
    private final Table<String> replicaTable = new Table<>("");
    private final Table<String> clientTable = new Table<>("");
    private final Label logLabel = new Label("");
    private final Label hintLabel = new Label("");
    private final Label statusLabel = new Label("");
    private final Label narrationLabel = new Label("");
    private final CommandBox commandBox = new CommandBox();
    private final BasicWindow window = new BasicWindow();

    Tui(int nReplicas, int nClients, int coordinator) throws Exception {
        this.nReplicas = nReplicas;
        this.nClients = nClients;
        this.coordinator = coordinator;
        Path logPath = Files.createTempFile("ds-tui", ".log");
        Logger.setDestinationFile(logPath.toString());
        logFile = new RandomAccessFile(logPath.toFile(), "r");
        boot(nReplicas, nClients, coordinator);
    }

    /** Stops any running scenario and boots a fresh system. */
    private void restart(int nReplicas, int nClients, int coordinator) {
        script.clear();
        narrationLabel.setText("");
        boot(nReplicas, nClients, coordinator);
    }

    /** (Re)creates the whole system from scratch, discarding the previous one. */
    private void boot(int nReplicas, int nClients, int coordinator) {
        if (system != null) system.terminate();
        try {
            logFile.seek(logFile.length()); // don't attribute the old system's lines to the new one
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        // Akka's own logger prints to stdout (e.g. dead letters after a crash), which would scribble over the screen.
        system = ActorSystem.create(
                "Tui", ConfigFactory.parseString("akka.loglevel = OFF\nakka.stdout-loglevel = OFF"));
        // New queue: late callbacks from the terminated system land in the old one, which nobody reads.
        Queue<Event> queue = events = new ConcurrentLinkedQueue<>();
        replicas.clear();
        clients.clear();
        replicaIds.clear();
        clientIds.clear();
        log.clear();

        ActorRef probe = system.actorOf(Props.create(Probe.class, () -> new Probe(queue)), "tui_probe");
        replicaViews = new ReplicaView[nReplicas];
        Map<Integer, ActorRef> group = new HashMap<>();
        for (int i = 0; i < nReplicas; i++) {
            ActorRef r = system.actorOf(
                    Replica.propsWithListener(
                            i,
                            AbstractReplica.MIN_LATENCY,
                            AbstractReplica.MAX_LATENCY,
                            AbstractReplica.COORDINATOR_BEAT_INTERVAL,
                            probe),
                    "Replica_" + i);
            replicas.add(r);
            replicaIds.put(r, i);
            group.put(i, r);
            replicaViews[i] = new ReplicaView();
            replicaViews[i].coordinator = coordinator;
        }
        InitSystem init = new InitSystem(group, coordinator);
        replicas.forEach(r -> r.tell(init, ActorRef.noSender()));

        clientViews = new ClientView[nClients];
        for (int i = 0; i < nClients; i++) {
            ActorRef c = system.actorOf(
                    Client.propsWithListener(READ_TIMEOUT_MS, WRITE_TIMEOUT_MS, Optional.empty(), probe),
                    "Client_" + i);
            clients.add(c);
            clientIds.put(c, i);
            clientViews[i] = new ClientView();
        }
    }

    public static void main(String[] args) throws Exception {
        int nReplicas = args.length > 0 ? Integer.parseInt(args[0]) : 5;
        int nClients = args.length > 1 ? Integer.parseInt(args[1]) : 3;
        int coordinator = args.length > 2 ? Integer.parseInt(args[2]) : 0;
        if (nReplicas < 1 || nClients < 1 || coordinator < 0 || coordinator >= nReplicas)
            throw new IllegalArgumentException("need replicas>=1, clients>=1, 0<=coordinator<replicas");

        Tui tui = new Tui(nReplicas, nClients, coordinator);
        try (Screen screen = new DefaultTerminalFactory().createScreen()) {
            screen.startScreen();
            tui.run(new MultiWindowTextGUI(screen));
        } finally {
            tui.system.terminate();
        }
    }

    // =================================================================================
    // UI
    // =================================================================================

    private void run(MultiWindowTextGUI gui) throws Exception {
        TextColor def = TextColor.ANSI.DEFAULT;
        gui.setTheme(SimpleTheme.makeTheme(true, def, def, def, def, TextColor.ANSI.BLACK, TextColor.ANSI.CYAN, def));

        replicaTable.setTableCellRenderer(new DefaultTableCellRenderer<>() {
            @Override
            protected void applyStyle(Table<String> t, String cell, int col, int row, boolean sel, TextGUIGraphics g) {
                g.applyThemeStyle(t.getThemeDefinition().getNormal()); // no selection: the table is display-only
                g.setForegroundColor(statusColor(t.getTableModel().getCell(1, row)));
            }
        });
        clientTable.setTableCellRenderer(new DefaultTableCellRenderer<>() {
            @Override
            protected void applyStyle(Table<String> t, String cell, int col, int row, boolean sel, TextGUIGraphics g) {
                g.applyThemeStyle(t.getThemeDefinition().getNormal());
            }
        });

        Panel top = new Panel(new LinearLayout(Direction.VERTICAL));
        top.addComponent(replicaTable.withBorder(Borders.singleLine("Replicas")));
        top.addComponent(clientTable.withBorder(Borders.singleLine("Clients")));

        Panel prompt = new Panel(new LinearLayout(Direction.HORIZONTAL));
        prompt.addComponent(new Label(">"));
        prompt.addComponent(
                commandBox, LinearLayout.createLayoutData(LinearLayout.Alignment.Fill, LinearLayout.GrowPolicy.CanGrow));
        Panel bottom = new Panel(new LinearLayout(Direction.VERTICAL));
        bottom.addComponent(hintLabel.setForegroundColor(TextColor.ANSI.BLACK_BRIGHT));
        bottom.addComponent(narrationLabel.setForegroundColor(TextColor.ANSI.YELLOW));
        bottom.addComponent(statusLabel.setForegroundColor(TextColor.ANSI.CYAN));
        bottom.addComponent(prompt, LinearLayout.createLayoutData(LinearLayout.Alignment.Fill));

        Panel root = new Panel(new BorderLayout());
        root.addComponent(top, BorderLayout.Location.TOP);
        root.addComponent(logLabel.withBorder(Borders.singleLine("Log")), BorderLayout.Location.CENTER);
        root.addComponent(bottom, BorderLayout.Location.BOTTOM);

        window.setHints(List.of(Window.Hint.FULL_SCREEN, Window.Hint.NO_DECORATIONS));
        window.setComponent(root);
        gui.addWindow(window);
        window.setFocusedInteractable(commandBox);
        commandBox.setTextChangeListener((text, byUser) -> hintLabel.setText(hint(text)));
        hintLabel.setText(hint(""));

        // Single thread: drain callbacks, then let Lanterna handle input, resizes and (delta) redraws.
        while (!gui.getWindows().isEmpty()) {
            while (!script.isEmpty() && System.currentTimeMillis() >= nextStepAt) step(script.poll());
            Event e;
            while ((e = events.poll()) != null) apply(e);
            tailLog();
            refreshWidgets();
            if (!gui.getGUIThread().processEventsAndUpdate()) Thread.sleep(20);
        }
    }

    private void step(String step) {
        String[] a = step.split(" ", 2);
        switch (a[0]) {
            case "wait" -> nextStepAt = System.currentTimeMillis() + Long.parseLong(a[1]);
            case "say" -> narrationLabel.setText(scenarioName + ": " + a[1]);
            default -> statusLabel.setText(execute(step));
        }
    }

    private static TextColor statusColor(String status) {
        if (status.startsWith("CRASHED")) return TextColor.ANSI.RED;
        if (status.startsWith("ELECTING")) return TextColor.ANSI.MAGENTA;
        if (status.startsWith("COORDINATOR")) return TextColor.ANSI.YELLOW;
        return TextColor.ANSI.GREEN;
    }

    private void refreshWidgets() {
        // Union of all indices any replica reported applying, one column each.
        SortedSet<Integer> indices = new TreeSet<>();
        for (ReplicaView v : replicaViews) indices.addAll(v.values.keySet());
        List<String> header = new ArrayList<>(List.of("ID", "STATUS", "COORD", "SCHEDULED CRASH"));
        indices.forEach(i -> header.add("[" + i + "]"));
        List<List<String>> rows = new ArrayList<>();
        for (int id = 0; id < replicaViews.length; id++) {
            ReplicaView v = replicaViews[id];
            String state = v.crashed ? "CRASHED" : v.electing ? "ELECTING" : v.coordinator == id ? "COORDINATOR" : "UP";
            List<String> row = new ArrayList<>(List.of("R" + id, state, "R" + v.coordinator, v.armed));
            indices.forEach(i -> row.add(Objects.toString(v.values.get(i), ".")));
            rows.add(row);
        }
        setTable(replicaTable, header, rows);

        rows = new ArrayList<>();
        for (int id = 0; id < clientViews.length; id++)
            rows.add(List.of("C" + id, String.valueOf(clientViews[id].pending), clientViews[id].last));
        setTable(clientTable, List.of("ID", "PENDING", "LAST RESULT"), rows);

        int height = logLabel.getSize().getRows();
        List<String> lines = new ArrayList<>(log);
        String text = String.join("\n", lines.subList(Math.max(0, lines.size() - height), lines.size()));
        if (!text.equals(logLabel.getText())) logLabel.setText(text);
    }

    /** Swap the model only when the content changed, so idle frames redraw nothing. */
    private static void setTable(Table<String> table, List<String> header, List<List<String>> rows) {
        TableModel<String> old = table.getTableModel();
        if (old.getColumnLabels().equals(header) && old.getRows().equals(rows)) return;
        TableModel<String> model = new TableModel<>(header.toArray(String[]::new));
        rows.forEach(model::addRow);
        table.setTableModel(model);
    }

    /** Single-line prompt with Up/Down history and Tab completion driven by {@link #USAGE}. */
    private final class CommandBox extends TextBox {
        private final List<String> history = new ArrayList<>();
        private int historyPos;

        CommandBox() {
            super(new TerminalSize(20, 1));
        }

        @Override
        public synchronized Result handleKeyStroke(KeyStroke k) {
            switch (k.getKeyType()) {
                case Enter -> {
                    String cmd = getText().trim();
                    if (!cmd.isEmpty() && (history.isEmpty() || !history.getLast().equals(cmd))) history.add(cmd);
                    historyPos = history.size();
                    replace("");
                    statusLabel.setText(execute(cmd));
                }
                case ArrowUp -> {
                    if (historyPos > 0) replace(history.get(--historyPos));
                }
                case ArrowDown -> {
                    historyPos = Math.min(historyPos + 1, history.size());
                    replace(historyPos < history.size() ? history.get(historyPos) : "");
                }
                case Escape -> replace("");
                case Tab -> complete();
                default -> {
                    return super.handleKeyStroke(k);
                }
            }
            return Result.HANDLED;
        }

        private void replace(String text) {
            setText(text);
            setCaretPosition(text.length());
        }

        private void complete() {
            String text = getText();
            String head = text.substring(0, text.lastIndexOf(' ') + 1);
            String word = text.substring(head.length());
            List<String> matches = matches(head, word);
            if (matches.size() == 1) replace(head + matches.getFirst() + " ");
            else if (!matches.isEmpty()) replace(head + commonPrefix(matches));
        }
    }

    /** Name of the argument being typed after {@code head} ("command" for the first word), or null. */
    private static String argName(String head) {
        String[] prev = head.isBlank() ? new String[0] : head.trim().split("\\s+");
        if (prev.length == 0) return "command";
        List<String> args = USAGE.get(ALIASES.getOrDefault(prev[0], prev[0]));
        return args == null || prev.length > args.size() ? null : args.get(prev.length - 1);
    }

    private List<String> matches(String head, String word) {
        String arg = argName(head);
        List<String> all =
                switch (arg == null ? "" : arg) {
                    case "command" -> List.copyOf(USAGE.keySet());
                    case "<client>" -> ids(clients.size());
                    case "<replica>" -> ids(replicas.size());
                    case "[type]" -> Arrays.stream(Crash.Type.values()).map(Enum::name).toList();
                    case "[scenario]" -> List.copyOf(Scenario.ALL.keySet());
                    default -> List.of();
                };
        return all.stream()
                .filter(c -> c.toLowerCase().startsWith(word.toLowerCase()))
                .toList();
    }

    private String hint(String text) {
        String head = text.substring(0, text.lastIndexOf(' ') + 1);
        String arg = argName(head);
        if (arg == null) return "Enter to run";
        List<String> m = matches(head, text.substring(head.length()));
        String next = arg.equals("command") ? HELP : arg + (m.isEmpty() ? "" : ": " + String.join(" ", m));
        return next + "   (Tab complete, Up/Down history, Esc clear)";
    }

    private static List<String> ids(int n) {
        return IntStream.range(0, n).mapToObj(String::valueOf).toList();
    }

    private static String commonPrefix(List<String> words) {
        String p = words.getFirst();
        for (String w : words)
            while (!w.toLowerCase().startsWith(p.toLowerCase())) p = p.substring(0, p.length() - 1);
        return p;
    }

    // =================================================================================
    // API callbacks and commands
    // =================================================================================

    private void apply(Event e) {
        switch (e.msg()) {
            case UpdateApplied u -> replicaViews[u.replicaId].values.put(u.index, u.value);
            case ElectionStarted s -> replicaViews[s.replicaId].electing = true;
            case CoordinatorElected c -> {
                replicaViews[c.replicaId].coordinator = c.newCoordinatorId;
                replicaViews[c.replicaId].electing = false;
            }
            case Crash c -> {
                ReplicaView v = replicaViews[replicaIds.get(e.from())];
                if (c.type == Crash.Type.Now) v.crashed = true;
                else v.armed = c.type + " after " + c.after_n_messages_of_type;
            }
            case ReadResult r -> clientDone(e.from(), "READ [" + r.index + "] = " + r.value + " from R" + r.fromReplica);
            case WriteResult w -> clientDone(
                    e.from(),
                    "WRITE [" + w.index + "] := " + w.value + (w.success ? " OK" : " FAILED") + " via R" + w.fromReplica);
            case ReadTimeout t -> clientDone(e.from(), "READ [" + t.index + "] TIMEOUT at " + t.replica.path().name());
            case WriteTimeout t -> clientDone(
                    e.from(), "WRITE [" + t.index + "] := " + t.value + " TIMEOUT at " + t.replica.path().name());
            default -> {}
        }
    }

    private void clientDone(ActorRef client, String what) {
        ClientView v = clientViews[clientIds.get(client)];
        v.pending = Math.max(0, v.pending - 1);
        v.last = what;
    }

    private String execute(String cmd) {
        if (cmd.isEmpty()) return "";
        String[] a = cmd.split("\\s+");
        try {
            switch (ALIASES.getOrDefault(a[0], a[0])) {
                case "read" -> {
                    int c = idx(a[1], clients), r = idx(a[2], replicas), i = Integer.parseInt(a[3]);
                    clients.get(c).tell(new ReadRequest(i, replicas.get(r)), ActorRef.noSender());
                    clientViews[c].pending++;
                    return "C" + c + " -> READ [" + i + "] at R" + r;
                }
                case "write" -> {
                    int c = idx(a[1], clients), r = idx(a[2], replicas);
                    int i = Integer.parseInt(a[3]), val = Integer.parseInt(a[4]);
                    clients.get(c).tell(new WriteRequest(i, val, replicas.get(r)), ActorRef.noSender());
                    clientViews[c].pending++;
                    return "C" + c + " -> WRITE [" + i + "] := " + val + " at R" + r;
                }
                case "crash" -> {
                    int r = idx(a[1], replicas);
                    Crash.Type type = a.length > 2 ? crashType(a[2]) : Crash.Type.Now;
                    int n = a.length > 3 ? Integer.parseInt(a[3]) : 1;
                    replicas.get(r).tell(new Crash(type, n), ActorRef.noSender());
                    return "crash R" + r + ": " + type + (type == Crash.Type.Now ? "" : " after " + n);
                }
                case "scenario" -> {
                    if (a.length == 1) return "scenarios: " + String.join(", ", Scenario.ALL.keySet());
                    Scenario s = Scenario.ALL.get(a[1]);
                    if (s == null) return "unknown scenario: " + a[1];
                    restart(s.replicas(), s.clients(), s.coordinator());
                    scenarioName = a[1];
                    narrationLabel.setText(a[1] + ": " + s.description());
                    script.addAll(s.steps());
                    nextStepAt = System.currentTimeMillis() + 1000; // let the system initialise first
                    return "running scenario " + a[1] + " on a fresh system";
                }
                case "reset" -> {
                    restart(nReplicas, nClients, coordinator);
                    return "fresh system started";
                }
                case "debug" -> {
                    debug = !debug;
                    Logger.setDebugEnabled(debug);
                    return "debug logs " + (debug ? "on" : "off");
                }
                case "quit" -> {
                    window.close();
                    return "";
                }
                default -> {
                    return "unknown command: " + a[0];
                }
            }
        } catch (ArrayIndexOutOfBoundsException ex) {
            return "missing arguments: " + a[0] + " " + String.join(" ", USAGE.get(ALIASES.getOrDefault(a[0], a[0])));
        } catch (IllegalArgumentException ex) {
            return "bad arguments (" + ex.getMessage() + ")";
        }
    }

    private static int idx(String s, List<?> range) {
        int i = Integer.parseInt(s.replaceFirst("^[rRcC]", ""));
        if (i < 0 || i >= range.size()) throw new IllegalArgumentException("id out of range: " + s);
        return i;
    }

    private static Crash.Type crashType(String s) {
        for (Crash.Type t : Crash.Type.values()) if (t.name().equalsIgnoreCase(s)) return t;
        throw new IllegalArgumentException("unknown crash type: " + s);
    }

    // Only the bare line: "CRASHED: <type> (n)" is AbstractReplica echoing the request, also when merely arming.
    private static final Pattern CRASHED = Pattern.compile("\\[Replica (\\d+)\\] CRASHED$");

    private void tailLog() throws Exception {
        String line;
        while ((line = logFile.readLine()) != null) {
            // readLine decodes as Latin-1; re-decode as the UTF-8 the Logger writes.
            line = new String(line.getBytes(StandardCharsets.ISO_8859_1), StandardCharsets.UTF_8);
            log.addLast(line.length() > 11 ? line.substring(11) : line); // drop the date, keep the time
            // A deferred crash fires no callback; the replica only logs it, so take it from there.
            Matcher m = CRASHED.matcher(line);
            if (m.find() && Integer.parseInt(m.group(1)) < replicaViews.length)
                replicaViews[Integer.parseInt(m.group(1))].crashed = true;
            if (log.size() > LOG_LINES_KEPT) log.removeFirst();
        }
    }
}
