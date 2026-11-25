package it.unitn.ds.tui;

import akka.actor.AbstractActor;
import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.actor.Props;
import com.googlecode.lanterna.TerminalSize;
import com.googlecode.lanterna.TextColor;
import com.googlecode.lanterna.graphics.SimpleTheme;
import com.googlecode.lanterna.graphics.ThemeDefinition;
import com.googlecode.lanterna.gui2.ActionListBox;
import com.googlecode.lanterna.gui2.AsynchronousTextGUIThread;
import com.googlecode.lanterna.gui2.BasicWindow;
import com.googlecode.lanterna.gui2.BorderLayout;
import com.googlecode.lanterna.gui2.Borders;
import com.googlecode.lanterna.gui2.Button;
import com.googlecode.lanterna.gui2.ComboBox;
import com.googlecode.lanterna.gui2.Component;
import com.googlecode.lanterna.gui2.Direction;
import com.googlecode.lanterna.gui2.GridLayout;
import com.googlecode.lanterna.gui2.Label;
import com.googlecode.lanterna.gui2.LinearLayout;
import com.googlecode.lanterna.gui2.MultiWindowTextGUI;
import com.googlecode.lanterna.gui2.Panel;
import com.googlecode.lanterna.gui2.Panels;
import com.googlecode.lanterna.gui2.SeparateTextGUIThread;
import com.googlecode.lanterna.gui2.TextBox;
import com.googlecode.lanterna.gui2.TextGUIGraphics;
import com.googlecode.lanterna.gui2.Window;
import com.googlecode.lanterna.gui2.WindowListenerAdapter;
import com.googlecode.lanterna.gui2.dialogs.ActionListDialogBuilder;
import com.googlecode.lanterna.gui2.table.DefaultTableCellRenderer;
import com.googlecode.lanterna.gui2.table.Table;
import com.googlecode.lanterna.gui2.table.TableModel;
import com.googlecode.lanterna.input.KeyStroke;
import com.googlecode.lanterna.input.KeyType;
import com.googlecode.lanterna.screen.Screen;
import com.googlecode.lanterna.terminal.DefaultTerminalFactory;
import com.googlecode.lanterna.terminal.MouseCaptureMode;
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
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
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
    private static final String KEYS =
            "↑↓/click: select replica   Enter: replica actions   r read   w write   c crash   s scenarios   x reset   d debug log   q quit";

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

    // All state below is only touched on the Lanterna GUI thread.
    private final int nReplicas, nClients, coordinator; // command-line setup, restored by reset
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
    private String lastIndex = "1", lastValue = "10"; // dialog defaults: the previous input

    // Running scenario and the index of its next step
    private Scenario scenario;
    private String scenarioName;
    private int stepIndex;
    private long nextStepAt;

    // Widgets
    private MultiWindowTextGUI gui;
    private final BasicWindow window = new BasicWindow();
    private final Table<String> replicaTable = new Table<>("");
    private final Table<String> clientTable = new Table<>("");
    private final Label scenarioLabel = new Label("");
    private final Label logLabel = new Label("");
    private final Label statusLabel = new Label("");

    Tui(int nReplicas, int nClients, int coordinator) throws Exception {
        this.nReplicas = nReplicas;
        this.nClients = nClients;
        this.coordinator = coordinator;
        Path logPath = Files.createTempFile("ds-tui", ".log");
        Logger.setDestinationFile(logPath.toString());
        logFile = new RandomAccessFile(logPath.toFile(), "r");
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
        DefaultTerminalFactory factory = new DefaultTerminalFactory().setMouseCaptureMode(MouseCaptureMode.CLICK_RELEASE);
        try (Screen screen = factory.createScreen()) {
            screen.startScreen();
            tui.run(new MultiWindowTextGUI(new SeparateTextGUIThread.Factory(), screen));
        } finally {
            tui.system.terminate();
        }
    }

    // =================================================================================
    // Layout and main loop
    // =================================================================================

    private void run(MultiWindowTextGUI gui) throws Exception {
        this.gui = gui;
        TextColor def = TextColor.ANSI.DEFAULT;
        gui.setTheme(SimpleTheme.makeTheme(
                true, def, def, TextColor.ANSI.WHITE, TextColor.ANSI.BLACK_BRIGHT, TextColor.ANSI.BLACK,
                TextColor.ANSI.CYAN, def));

        replicaTable.setTableCellRenderer(new DefaultTableCellRenderer<>() {
            @Override
            protected void applyStyle(Table<String> t, String cell, int col, int row, boolean sel, TextGUIGraphics g) {
                ThemeDefinition theme = t.getThemeDefinition();
                if (sel) {
                    g.applyThemeStyle(t.isFocused() ? theme.getActive() : theme.getSelected());
                } else {
                    g.applyThemeStyle(theme.getNormal());
                    g.setForegroundColor(statusColor(t.getTableModel().getCell(1, row)));
                }
            }
        });
        replicaTable.setSelectAction(this::replicaMenu);
        clientTable.setTableCellRenderer(new DefaultTableCellRenderer<>() {
            @Override
            protected void applyStyle(Table<String> t, String cell, int col, int row, boolean sel, TextGUIGraphics g) {
                g.applyThemeStyle(t.getThemeDefinition().getNormal());
            }
        });
        clientTable.setEnabled(false); // display only: keep it out of the focus cycle

        Panel actions = Panels.horizontal(
                new Button("Read", () -> requestDialog(false)),
                new Button("Write", () -> requestDialog(true)),
                new Button("Crash", this::crashDialog),
                new Button("Scenarios", this::scenarioPicker),
                new Button("Reset", this::reset),
                new Button("Debug log", this::toggleDebug),
                new Button("Quit", window::close));

        Panel left = new Panel(new LinearLayout(Direction.VERTICAL));
        left.addComponent(replicaTable.withBorder(Borders.singleLine("Replicas")));
        left.addComponent(clientTable.withBorder(Borders.singleLine("Clients")));
        left.addComponent(actions);

        Panel top = new Panel(new LinearLayout(Direction.HORIZONTAL));
        top.addComponent(left);
        top.addComponent(
                scenarioLabel.withBorder(Borders.singleLine("Scenario")),
                LinearLayout.createLayoutData(LinearLayout.Alignment.Fill, LinearLayout.GrowPolicy.CanGrow));

        Panel bottom = Panels.vertical(
                statusLabel.setForegroundColor(TextColor.ANSI.CYAN),
                new Label(KEYS).setForegroundColor(TextColor.ANSI.BLACK_BRIGHT));

        Panel root = new Panel(new BorderLayout());
        root.addComponent(top, BorderLayout.Location.TOP);
        root.addComponent(logLabel.withBorder(Borders.singleLine("Log")), BorderLayout.Location.CENTER);
        root.addComponent(bottom, BorderLayout.Location.BOTTOM);

        window.setHints(List.of(Window.Hint.FULL_SCREEN, Window.Hint.NO_DECORATIONS));
        window.setComponent(root);
        window.addWindowListener(new WindowListenerAdapter() {
            @Override
            public void onUnhandledInput(Window w, KeyStroke k, AtomicBoolean handled) {
                if (k.getKeyType() == KeyType.Character) handled.set(hotkey(Character.toLowerCase(k.getCharacter())));
            }
        });
        gui.addWindow(window);
        window.setFocusedInteractable(replicaTable);
        tick();

        // Lanterna runs input, dialogs and redraws on its own thread; the ticker feeds that same thread,
        // so the model is single-threaded and keeps updating while a dialog is open.
        AsynchronousTextGUIThread guiThread = (AsynchronousTextGUIThread) gui.getGUIThread();
        ScheduledExecutorService ticker = Executors.newSingleThreadScheduledExecutor();
        guiThread.start();
        ticker.scheduleAtFixedRate(() -> guiThread.invokeLater(this::tick), 50, 50, TimeUnit.MILLISECONDS);
        window.waitUntilClosed();
        ticker.shutdownNow();
        guiThread.stop();
    }

    private boolean hotkey(char c) {
        switch (c) {
            case 'r' -> requestDialog(false);
            case 'w' -> requestDialog(true);
            case 'c' -> crashDialog();
            case 's' -> scenarioPicker();
            case 'x' -> reset();
            case 'd' -> toggleDebug();
            case 'q' -> window.close();
            default -> {
                return false;
            }
        }
        return true;
    }

    private void tick() {
        long now = System.currentTimeMillis();
        while (scenario != null && stepIndex < scenario.steps().size() && now >= nextStepAt)
            step(scenario.steps().get(stepIndex++));
        Event e;
        while ((e = events.poll()) != null) apply(e);
        tailLog();
        refreshWidgets();
    }

    // =================================================================================
    // Actions and dialogs
    // =================================================================================

    private int selectedReplica() {
        return Math.min(replicaTable.getSelectedRow(), replicas.size() - 1);
    }

    /** Enter or click on a replica row. */
    private void replicaMenu() {
        int r = selectedReplica();
        new ActionListDialogBuilder()
                .setTitle("Replica R" + r)
                .setDescription("Choose an action")
                .setCanCancel(true)
                .addAction("Read from R" + r + "…", () -> requestDialog(false))
                .addAction("Write through R" + r + "…", () -> requestDialog(true))
                .addAction("Crash R" + r + "…", this::crashDialog)
                .build()
                .showDialog(gui);
    }

    private void requestDialog(boolean write) {
        int r = selectedReplica();
        ComboBox<String> client = new ComboBox<>(IntStream.range(0, clients.size()).mapToObj(i -> "C" + i).toList(), 0);
        TextBox index = numberBox(lastIndex), value = numberBox(lastValue);
        Map<String, Component> fields = new LinkedHashMap<>();
        fields.put("Client", client);
        fields.put("Index (0-" + (AbstractReplica.POSITIONS_LIST_LENGTH - 1) + ")", index);
        if (write) fields.put("Value", value);
        if (!form(write ? "Write through R" + r : "Read from R" + r, "Pick a replica in the table to change the target.", fields))
            return;
        lastIndex = index.getText();
        lastValue = value.getText();
        String args = client.getSelectedIndex() + " " + r + " " + index.getText();
        statusLabel.setText(execute(write ? "write " + args + " " + value.getText() : "read " + args));
    }

    private void crashDialog() {
        int r = selectedReplica();
        ComboBox<String> type = new ComboBox<>(Arrays.stream(Crash.Type.values()).map(Enum::name).toList(), 0);
        TextBox n = numberBox("1");
        Map<String, Component> fields = new LinkedHashMap<>();
        fields.put("When", type);
        fields.put("After n messages", n);
        String help = "Now: stops at once. Other types: crashes after handling n messages of that type.\n"
                + "A crashed replica never comes back: use Reset to start over.";
        if (!form("Crash R" + r, help, fields)) return;
        statusLabel.setText(execute("crash " + r + " " + type.getSelectedItem() + " " + n.getText()));
    }

    private static TextBox numberBox(String initial) {
        return new TextBox(new TerminalSize(8, 1), initial).setValidationPattern(Pattern.compile("\\d{1,9}"));
    }

    /** Modal form; returns true on OK. Enter moves to the next field, Esc cancels. */
    private boolean form(String title, String help, Map<String, Component> fields) {
        BasicWindow w = new BasicWindow(title);
        w.setHints(List.of(Window.Hint.MODAL, Window.Hint.CENTERED));
        w.setCloseWindowWithEscape(true);
        Panel grid = new Panel(new GridLayout(2));
        fields.forEach((label, c) -> {
            grid.addComponent(new Label(label));
            grid.addComponent(c);
        });
        boolean[] ok = {false};
        Button submit = new Button("OK", () -> {
            ok[0] = true;
            w.close();
        });
        w.setComponent(Panels.vertical(
                new Label(help).setForegroundColor(TextColor.ANSI.BLACK_BRIGHT),
                grid,
                Panels.horizontal(submit, new Button("Cancel", w::close))));
        gui.addWindowAndWait(w);
        return ok[0];
    }

    private void scenarioPicker() {
        List<String> names = List.copyOf(Scenario.ALL.keySet());
        BasicWindow w = new BasicWindow("Scenarios");
        w.setHints(List.of(Window.Hint.MODAL, Window.Hint.CENTERED));
        w.setCloseWindowWithEscape(true);
        Label preview = new Label("");
        // Fixed size (the largest preview) so the dialog doesn't jump around while browsing.
        int cols = 0, rows = 0;
        for (String name : names) {
            String[] lines = describe(name, -1).split("\n");
            rows = Math.max(rows, lines.length);
            for (String l : lines) cols = Math.max(cols, l.length());
        }
        preview.setPreferredSize(new TerminalSize(cols, rows));
        ActionListBox list = new ActionListBox() {
            @Override
            public Result handleKeyStroke(KeyStroke k) {
                Result result = super.handleKeyStroke(k);
                preview.setText(describe(names.get(getSelectedIndex()), -1));
                return result;
            }
        };
        names.forEach(name -> list.addItem(name, () -> {
            w.close();
            runScenario(name);
        }));
        preview.setText(describe(names.getFirst(), -1));
        w.setComponent(Panels.vertical(
                Panels.horizontal(list.withBorder(Borders.singleLine()), preview.withBorder(Borders.singleLine())),
                new Label("↑↓ browse   Enter/click run (restarts the system)   Esc cancel")
                        .setForegroundColor(TextColor.ANSI.BLACK_BRIGHT)));
        gui.addWindowAndWait(w);
    }

    /**
     * Scenario description and its diagram. Steps before {@code current} are ticked, {@code current} gets an arrow;
     * -1 marks nothing.
     */
    private static String describe(String name, int current) {
        Scenario s = Scenario.ALL.get(name);
        StringBuilder b = new StringBuilder(name + ": " + s.description() + "\n" + s.setup() + "\n\n");
        b.append("   ").append(s.lanes()).append('\n');
        List<String> diagram = s.diagram();
        for (int i = 0; i < diagram.size(); i++) {
            String gutter = i < current ? " ✓ " : i == current ? " ► " : "   ";
            b.append(gutter).append(diagram.get(i).replace("\n", "\n   ")).append('\n');
        }
        return b.append('\n').append(Scenario.LEGEND).toString();
    }

    private void runScenario(String name) {
        Scenario s = Scenario.ALL.get(name);
        restart(s.replicas(), s.clients(), s.coordinator());
        scenario = s;
        scenarioName = name;
        nextStepAt = System.currentTimeMillis() + 1000; // let the system initialise first
        statusLabel.setText("running scenario " + name + " on a fresh system");
    }

    private void reset() {
        restart(nReplicas, nClients, coordinator);
        statusLabel.setText("all replicas and clients restarted from the initial state");
    }

    /** Stops any running scenario and boots a fresh system. */
    private void restart(int nReplicas, int nClients, int coordinator) {
        scenario = null;
        stepIndex = 0;
        boot(nReplicas, nClients, coordinator);
        replicaTable.setSelectedRow(0);
    }

    private void toggleDebug() {
        debug = !debug;
        Logger.setDebugEnabled(debug);
        statusLabel.setText("debug logs " + (debug ? "on" : "off"));
    }

    private void step(String step) {
        String[] a = step.split(" ", 2);
        switch (a[0]) {
            case "wait" -> nextStepAt = System.currentTimeMillis() + Long.parseLong(a[1]);
            case "say" -> {}
            default -> statusLabel.setText(execute(step));
        }
    }

    // =================================================================================
    // Rendering
    // =================================================================================

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

        String text;
        if (scenario == null) {
            text = "No scenario running.\nPress s (or Scenarios) to pick one: each shows what will happen before it starts.";
        } else {
            boolean finished = stepIndex == scenario.steps().size() && System.currentTimeMillis() >= nextStepAt;
            // State first: long descriptions wrap and may push the bottom lines out of the panel.
            text = (finished ? "FINISHED  " : "RUNNING  ") + describe(scenarioName, finished ? stepIndex : stepIndex - 1);
        }
        setText(scenarioLabel, text);

        int height = logLabel.getSize().getRows();
        List<String> lines = new ArrayList<>(log);
        setText(logLabel, String.join("\n", lines.subList(Math.max(0, lines.size() - height), lines.size())));
    }

    /** Only touch widgets whose content changed, so idle frames redraw nothing. */
    private static void setText(Label label, String text) {
        if (!text.equals(label.getText())) label.setText(text);
    }

    private static void setTable(Table<String> table, List<String> header, List<List<String>> rows) {
        TableModel<String> old = table.getTableModel();
        if (old.getColumnLabels().equals(header) && old.getRows().equals(rows)) return;
        TableModel<String> model = new TableModel<>(header.toArray(String[]::new));
        rows.forEach(model::addRow);
        table.setTableModel(model);
    }

    // =================================================================================
    // API callbacks and actions
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

    /** Sends one API request: {@code read c r idx}, {@code write c r idx val} or {@code crash r [type] [n]}. */
    private String execute(String cmd) {
        String[] a = cmd.split("\\s+");
        try {
            switch (a[0]) {
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
                    Crash.Type type = a.length > 2 ? Crash.Type.valueOf(a[2]) : Crash.Type.Now;
                    int n = a.length > 3 ? Integer.parseInt(a[3]) : 1;
                    replicas.get(r).tell(new Crash(type, n), ActorRef.noSender());
                    return "crash R" + r + ": " + type + (type == Crash.Type.Now ? "" : " after " + n);
                }
                default -> throw new IllegalArgumentException("unknown action " + a[0]);
            }
        } catch (ArrayIndexOutOfBoundsException | IllegalArgumentException ex) {
            return "invalid input (" + ex.getMessage() + ")";
        }
    }

    private static int idx(String s, List<?> range) {
        int i = Integer.parseInt(s);
        if (i < 0 || i >= range.size()) throw new IllegalArgumentException("id out of range: " + s);
        return i;
    }

    // Only the bare line: "CRASHED: <type> (n)" is AbstractReplica echoing the request, also when merely arming.
    private static final Pattern CRASHED = Pattern.compile("\\[Replica (\\d+)\\] CRASHED$");

    private void tailLog() {
        try {
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
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }
}
