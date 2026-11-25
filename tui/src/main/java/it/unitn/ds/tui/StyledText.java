package it.unitn.ds.tui;

import com.googlecode.lanterna.SGR;
import com.googlecode.lanterna.TerminalSize;
import com.googlecode.lanterna.TerminalTextUtils;
import com.googlecode.lanterna.TextColor;
import com.googlecode.lanterna.gui2.AbstractComponent;
import com.googlecode.lanterna.gui2.ComponentRenderer;
import com.googlecode.lanterna.gui2.TextGUIGraphics;
import java.util.List;

/** Read-only multi-line text whose runs each have their own colors (a Label has one color for everything). */
final class StyledText extends AbstractComponent<StyledText> {

    record Seg(String text, TextColor fg, TextColor bg, boolean bold) {
        Seg(String text, TextColor fg) {
            this(text, fg, TextColor.ANSI.DEFAULT, false);
        }
    }

    private List<List<Seg>> lines = List.of();

    /** Redraws only on change, so idle frames send nothing to the terminal. */
    void setLines(List<List<Seg>> lines) {
        if (lines.equals(this.lines)) return;
        this.lines = List.copyOf(lines);
        invalidate();
    }

    @Override
    protected ComponentRenderer<StyledText> createDefaultRenderer() {
        return new ComponentRenderer<>() {
            @Override
            public TerminalSize getPreferredSize(StyledText c) {
                int width = 0;
                for (List<Seg> line : c.lines)
                    width = Math.max(
                            width,
                            line.stream()
                                    .mapToInt(s -> TerminalTextUtils.getColumnWidth(s.text()))
                                    .sum());
                return new TerminalSize(width, c.lines.size());
            }

            @Override
            public void drawComponent(TextGUIGraphics g, StyledText c) {
                g.applyThemeStyle(c.getThemeDefinition().getNormal());
                g.fill(' ');
                for (int y = 0; y < c.lines.size(); y++) {
                    int x = 0;
                    for (Seg s : c.lines.get(y)) {
                        g.setForegroundColor(s.fg());
                        g.setBackgroundColor(s.bg());
                        if (s.bold()) g.enableModifiers(SGR.BOLD);
                        else g.disableModifiers(SGR.BOLD);
                        g.putString(x, y, s.text());
                        x += TerminalTextUtils.getColumnWidth(s.text());
                    }
                }
            }
        };
    }
}
