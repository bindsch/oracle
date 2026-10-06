# AGENTS.MD

Oracle-specific notes:

- ChatGPT project URLs: steipete@gmail.com -> https://chatgpt.com/g/g-p-691edc9fec088191b553a35093da1ea8-oracle/project; studpete@gmail.com -> https://chatgpt.com/g/g-p-69505ed97e3081918a275477a647a682/project. Prefer studpete URL if steipete project not found.
- Pro browser runs: allow up to 10 minutes; never click "Answer now"; keep at least 1–2 Pro live tests (reattach must stay Pro); move other tests to faster models where safe.
- Live smoke tests: OpenAI live tests are opt-in. Run `ORACLE_LIVE_TEST=1 pnpm vitest run tests/live/openai-live.test.ts` with a real `OPENAI_API_KEY` when you need the background path; gpt-5-pro can take ~10 minutes.
- Wait defaults: gpt-5-pro API runs detach by default; use `--wait` to stay attached. gpt-5.1 and browser runs block by default; every run prints `oracle session <id>` for reattach.
- Session storage: Oracle stores session data under `~/.oracle`; delete it if you need a clean slate.
- CLI output: the first line of any top-level CLI start banner should use the oracle emoji, e.g. `🧿 oracle (<version>) ...`; keep it only for the initial command headline. Exception: the TUI exit message also keeps the emoji.
- Model access note (2025-11-23): gpt-5.1-pro and grok-4.1 are not yet available on Peter’s keys; live tests that require them will fail until access is granted.
- Oracle CLI on Node 25: if `pnpm dlx @steipete/oracle --help` fails with a missing `node_sqlite3.node`, rebuild sqlite3 in the pnpm dlx cache using system Python: `PYTHON=/usr/bin/python3 /Users/steipete/Projects/oracle/runner npx node-gyp rebuild` from the sqlite3 package dir printed in the error, then rerun the command.
- Before a release, skim manual smokes in `docs/manual-tests.md` and rerun any that cover your change surface (especially browser/serve paths).
- If browser smokes echo the prompt (Instant), rerun with `--browser-keep-browser --verbose` in tmux, then inspect DOM with `pnpm tsx scripts/browser-tools.ts eval ...` to confirm assistant turns exist; we fixed a case by refreshing assistant snapshots post-send.
- Browser “Pro thinking” gate: never click/auto-click ChatGPT’s “Answer now” button. Treat it as a placeholder and wait 10m–1h for the real assistant response (auto-clicking skips long thinking and changes behavior).
- Browser smokes should preserve Markdown (lists, fences); if output looks flattened or echoed, inspect the captured assistant turn via `browser-tools.ts eval` before shipping.
- Working on Windows? Read and update `docs/windows-work.md` before you start.
- Sparkle signing key lives at `/Users/steipete/Library/CloudStorage/Dropbox/Backup/Sparkle`; set `SPARKLE_PRIVATE_KEY_FILE` to that path when notarizing the notifier.
- Browser cookie sync + Node 25: if browser runs fail with “Failed to load keytar… Cannot find module '../build/Release/keytar.node'” and no cookies are applied, rebuild keytar in the pnpm dlx cache: run `PYTHON=/usr/bin/python3 /Users/steipete/Projects/oracle/runner npx node-gyp rebuild` inside the keytar directory printed in the error, then rerun the oracle command.
- npm publish OTP: prepare/tag/release first, then run `npm publish ...` and stop at `Enter OTP:`; ask user for the OTP and continue (ok to handle OTP in chat).

Browser-mode debug notes (ChatGPT URL override)

- When a ChatGPT folder/workspace URL is set, Cloudflare can block automation even after cookie sync. Use `--browser-keep-browser` to leave Chrome open, solve the interstitial manually, then rerun.
- If a run stalls/looks finished but CLI didn’t stream output, check the latest session (`oracle status`) and open it (`oracle session <id> --render`) to confirm completion.
- Active Chrome port/pid live in session metadata (`~/.oracle/sessions/<id>/meta.json`). Connect with `npx tsx scripts/browser-tools.ts eval --port <port> "({ href: window.location.href, ready: document.readyState })"` to inspect the page.
- To debug with agent-tools, launch Chrome via an Oracle browser run (cookies copied) and keep it open (`--browser-keep-browser`). Then use `~/Projects/agent-scripts/bin/browser-tools ... --port <port>` with the port from `~/.oracle/sessions/<id>/meta.json`. Avoid starting a fresh browser-tools Chrome when you need the synced cookies.
- Double-hop nav is implemented (root then target URL), but Cloudflare may still need manual clearance or inline cookies.
- After finishing a feature, ask whether it matters to end users; if yes, update the changelog. Read the top ~100 lines first and group related edits into one entry instead of scattering multiple bullets.
- Beta publishing: when asked to ship a beta to npm, bump the version with a beta suffix (e.g., `0.4.4-beta.1`) before publishing; npm will not let you overwrite an existing beta tag without a new version.

Fork maintenance (bindsch/oracle over steipete/oracle)

- This checkout is a fork: `origin` = bindsch/oracle, `upstream` = steipete/oracle. The global `oracle` command runs THIS repo's `dist/` (launcher `~/Library/pnpm/oracle` hardcodes `ORACLE_ROOT`). After any source change you MUST `pnpm run build` for it to take effect — but NEVER rebuild while an oracle run is live; swapping `dist/` mid-run breaks in-flight jobs.
- Sync workflow: snapshot main (`git branch fork-snapshot-pre-sync-<sha>`), then `git rebase --onto upstream/main <old-base> main`. Resolve conflicts by preferring upstream for browser-capture/completion logic (upstream reworks it aggressively) and keeping only the fork patches below. After sync: typecheck, lint, build, full `vitest run`, then ONE live send smoke (`oracle -p "reply PING" --slug ...`) — commit verification is where env-specific send regressions surface.
- The fork carries exactly three patches on top of upstream; keep them, drop everything else as superseded:
  1. Cross-process rate limiter (`src/browser/rateLimiter.ts` + `index.ts`), `ORACLE_MAX_PARALLEL_BROWSER_SESSIONS` default 8 — caps parallel CLI invocations. No upstream equivalent.
  2. Chrome-claim startup mutex (`rateLimiter.ts` + `index.ts`) — serializes the launch/tab-claim window across processes; released at prompt submit. No upstream equivalent.
  3. Element-targeted DOM send click (`promptComposer.ts`) — since upstream #282 the send button is clicked with a trusted CDP event at viewport coords, which is swallowed by oracle's off-screen/hidden Chrome (shared profile, parallel runs) and fails commit verification ("Prompt did not appear ... send may have failed"). The fork clicks the button element directly with `dispatchClickSequence`. Upstream #302's focus-emulation + fallback did NOT fix it here. If a future upstream release makes trusted input land reliably in hidden windows, this patch can be dropped — verify with a live send first.
  4. Best-effort model/effort selection (`index.ts`, both local + remote paths) — `modelStrategy: "select"` hard-failed the whole run when the picker couldn't match the desired model. On a picker miss *with a valid session*, the fork logs a warning and continues with the current model/effort instead of throwing (a missing session still fails hard). Makes "select" a real default, not a lock. Drop if upstream adds a non-fatal select mode.

- ChatGPT project URL is per-account: `browser.chatgptUrl` must be the project ID for the account logged into the configured Chrome profile. A wrong/inaccessible project ID silently **redirects to the home page**, so conversations land in default chats instead of the project. Discover the right ID from the logged-in session: `fetch("/api/auth/session")` for the access token, then `GET /backend-api/gizmos/snorlax/sidebar` (Bearer) lists the account's projects with their `g-p-...` ids. The current project URL and Chrome profile are set in `~/.oracle/config.json` (`browser.chatgptUrl`); they are account-specific and not recorded here.
- Known limitation: oracle's model picker can't select ChatGPT's GPT-5.6 "Sol" label (it's behind a current-version submenu; upstream reworking matching in #317). To default to Sol, set it as the ChatGPT project's default model in the UI — the best-effort selection then lands on it.
