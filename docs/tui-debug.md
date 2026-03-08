# TUI navigation crash notes (2025-11-19)

Past issue

- The session selector could crash when the pointer landed on non-selectable rows (headers/separators) during fast navigation.

Mitigations implemented

- Start the list with a selectable row (“ask oracle”) so focus never begins on a header.
- Render table headers as disabled choices instead of separators so navigation skips them cleanly.
- Keep paging to on-screen “Older/Newer” actions; rely on those instead of PageUp/PageDown.

Open follow-ups

- Consider a custom prompt wrapper (outside Inquirer’s list) for key handling to avoid relying on private UI internals.
- Add a unit test that simulates rapid navigation to catch regressions.
