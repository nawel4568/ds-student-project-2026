package it.unitn.ds.tui;

import com.googlecode.lanterna.SGR;
import com.googlecode.lanterna.TerminalPosition;
import com.googlecode.lanterna.TerminalSize;
import com.googlecode.lanterna.TerminalTextUtils;
import com.googlecode.lanterna.TextColor;
import com.googlecode.lanterna.gui2.Button;
import com.googlecode.lanterna.gui2.TextGUIGraphics;
import com.googlecode.lanterna.gui2.Window;
import com.googlecode.lanterna.gui2.WindowListenerAdapter;
import com.googlecode.lanterna.input.KeyStroke;
import com.googlecode.lanterna.input.KeyType;
import com.googlecode.lanterna.input.MouseAction;
import java.util.concurrent.atomic.AtomicBoolean;

/** Button that lights up while the mouse is over it (needs mouse-move capture and {@link #track}). */
final class HoverButton extends Button {
    private static HoverButton hovered; // the pointer is in one place: at most one (GUI thread only)

    HoverButton(String label, Runnable action) {
        super(label, action);
    }

    /**
     * Lanterna only tells the component under the pointer about a move, never the one it left: so every move first
     * clears the hover, then the button under the pointer (if any) claims it again.
     */
    static void track(Window window) {
        window.addWindowListener(new WindowListenerAdapter() {
            @Override
            public void onInput(Window w, KeyStroke k, AtomicBoolean deliver) {
                if (k.getKeyType() == KeyType.MouseEvent && ((MouseAction) k).isMouseMove() && hovered != null) {
                    hovered.invalidate();
                    hovered = null;
                }
            }
        });
    }

    @Override
    public synchronized Result handleKeyStroke(KeyStroke k) {
        if (!isMouseMove(k)) return super.handleKeyStroke(k);
        hovered = this;
        invalidate();
        return Result.HANDLED;
    }

    @Override
    protected ButtonRenderer createDefaultRenderer() {
        return new ButtonRenderer() {
            @Override
            public TerminalPosition getCursorLocation(Button button) {
                return null;
            }

            @Override
            public TerminalSize getPreferredSize(Button button) {
                return new TerminalSize(TerminalTextUtils.getColumnWidth(button.getLabel()) + 4, 1);
            }

            @Override
            public void drawComponent(TextGUIGraphics g, Button button) {
                boolean hover = hovered == button, focus = button.isFocused();
                g.setForegroundColor(hover ? TextColor.ANSI.BLACK : focus ? TextColor.ANSI.WHITE_BRIGHT : TextColor.ANSI.CYAN);
                g.setBackgroundColor(
                        hover ? TextColor.ANSI.CYAN_BRIGHT : focus ? TextColor.ANSI.BLUE : TextColor.ANSI.DEFAULT);
                if (hover || focus) g.enableModifiers(SGR.BOLD);
                g.fill(' ');
                g.putString(0, 0, "[ " + button.getLabel() + " ]");
            }
        };
    }
}
