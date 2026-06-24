# DTS SU26 Contribution Journal

---

## Contribution 1: [`feat: Respect 'RateLimit' headers in default REST backoff implementation`](https://github.com/meltano/sdk/issues/2012)

| Field | Details |
|---|---|
| **Contribution Number** | 1 |
| **Student** | Saurav Rijal |
| **Issue** | [feat: Respect 'RateLimit' headers in default REST backoff implementation #2012](https://github.com/meltano/sdk/issues/2012) |
| **Forked Project** | [Rizsaurav/sdk](https://github.com/Rizsaurav/sdk) |
| **Status** | Phase III Complete (Build) |

---

## Why I Chose This Issue

I chose this issue because it directly addresses core backend networking resilience and API integration mechanics. In real-world enterprise environments, platforms must interface seamlessly with client architecture that is heavily rate-limited or structurally unstable. Contributing to Meltano's foundational SDK allows me to work directly on framework-level resilience algorithms rather than basic peripheral applications.

This task maps cleanly to my proficiency in full-stack Python development while pushing me to understand low-level HTTP/1.1 protocol handling and framework design. I hope to master parameter-driven backoff logic, learn how to defensively parse variable upstream server signals (like standard and custom headers), and gain experience contributing production-grade utilities to a widely used open-source data engineering platform.

---

## Understanding the Issue

### Problem Description

When building data extraction pipelines (Taps) with the Meltano Singer SDK, target upstream REST APIs will frequently throttle traffic and return an HTTP `429 Too Many Requests` response. Currently, the SDK's default REST stream framework automatically invokes a blind exponential backoff calculation to determine how long the system should stall before retrying the request. This ignores explicit timing instructions sent directly by the target server via standard rate-limiting headers, resulting in either unnecessary pipeline idle time or premature retry bursts that worsen API rate-limit exhaustion.

### Expected Behavior

The default REST stream implementation should defensively inspect incoming HTTP response headers when a retriable error or `429` status code occurs. If standard throttling headers — such as `Retry-After`, `X-RateLimit-Reset`, or `X-RateLimit-Reset-After` — are present, the backoff engine should dynamically extract and compute the exact sleep duration specified by the server. It should only default to standard exponential math ($2^n$) if these headers are completely missing or malformed.

### Current Behavior

The SDK currently forces an exponential wait time generator (`backoff.expo`) out-of-the-box. Unless a developer writes a custom, highly repetitive wrapper function to extract headers inside their individual data tap subclasses, the platform blindly guesses the wait window.

### Affected Components

The core components live in the REST stream orchestration module. Verified during Phase II by reading the source: the backoff methods are defined on the internal base class **`_HTTPStream`** (line 156), which both `RESTStream` and `GraphQLStream` inherit — so a default fix here benefits both stream types.

- `singer_sdk/streams/rest.py`:
  - `_HTTPStream.backoff_wait_generator` (line 855) — **root cause**; returns `backoff.expo(factor=2)`, a header-blind generator.
  - `_HTTPStream.backoff_runtime` (line 934) — existing `.send()`-protocol hook that exposes the thrown exception (and its `.response`); the intended mechanism for header-aware waits.
  - `_HTTPStream.request_decorator` (line 388) — wires the generator into `backoff.on_exception`.
  - `_HTTPStream.validate_response` (line 311) — raises `RetriableAPIError(msg, response)` on `429`, carrying the headers.
  - `singer_sdk/exceptions.py` — `RetriableAPIError.response` (defaults to `None`).

---

## Reproduction Process

### Environment Setup

Completed in Phase II on macOS (Apple Silicon), Python 3.11 host. The repo uses `uv` + `nox` (not `pip install -e`), per `docs/CONTRIBUTING.md`:

```bash
brew install uv                       # toolchain (was missing)
uv tool install pre-commit nox        # dev tools
uv sync --all-groups --all-extras     # build .venv with full dev deps
uv run pytest tests/core/rest/test_failure.py -q   # smoke test → 13 passed
```

Setup completed well under the viability threshold; the issue is confirmed viable.

### Steps to Reproduce

This is a **feature request**, so reproduction *demonstrates the current shortcoming*: the default backoff ignores server-provided rate-limit headers. The reproduction is deterministic and offline (no network).

1. Build a default `RESTStream` (no custom backoff overrides).
2. Construct an HTTP `429` response carrying explicit timing headers — `Retry-After: 15` and `X-RateLimit-Reset: 42` — wrapped in a `RetriableAPIError` (exactly what `validate_response` raises on `429`).
3. Ask the default `backoff_wait_generator()` for its wait schedule.
4. Compare the schedule produced **with** the headers vs. **without** them.

### Expected vs. Actual

| | |
|---|---|
| **Expected** (per issue #2012) | First wait ≈ **15s**, derived from `Retry-After` / `X-RateLimit-Reset`. |
| **Actual** | Blind exponential `2, 4, 8, 16, …` (`backoff.expo(factor=2)`); header values never read. |

### Reproduction Evidence

Reproduced via a standalone script run against the cloned SDK source (run **twice — identical output**, confirming it triggers 100% of the time):

```text
Server 429 response headers : {'Retry-After': '15', 'X-RateLimit-Reset': '42'}
RetriableAPIError.response   : status=429
  -> Server is telling us:  Retry-After=15s,  X-RateLimit-Reset=42

Default backoff_wait_generator() first 5 waits: [None, 2, 4, 8, 16]
  -> EXPECTED (per #2012): first wait ~= 15 (from Retry-After header)
  -> ACTUAL              : exponential 2,4,8,16,32 — headers ignored

Same generator, header-blind by construction:
  no-header run   : [None, 2, 4, 8, 16]
  with-header run : [None, 2, 4, 8, 16]
  identical?       : True  (proves headers are never read)

validate_response() raised: 429 Client Error: Too Many Requests for path: /dummy, headers: Retry-After
```

- **Proof it is header-blind:** the with-header and no-header runs are byte-identical, so the generator structurally cannot read the response.
- **Note on the leading `None`:** this is `backoff.expo`'s prime/sentinel slot — the `backoff` library primes the generator with `.send(None)` before sending the exception via `.send(exc)`. The meaningful schedule is `2, 4, 8, 16, 32` and is header-independent.
- **Branch:** `fix-issue-2012` on [Rizsaurav/sdk](https://github.com/Rizsaurav/sdk) — created off `main`. Per a deliberate "verify before pushing" policy, the illustrative reproduction **test** (this scenario flipped into a passing assertion) and the fix are committed to the fork in Phase III; the branch is not pushed with empty/unverified content.

---

## Solution Approach

> **Note:** This plan is a *living document* and may change as Phase III implementation reveals new details. An upstream PR (**#3672**) is already open against this issue using a convergent approach; I will reconcile naming and avoid duplication before submitting my own PR.

### Analysis (root cause, traced)

`_HTTPStream.backoff_wait_generator` (`singer_sdk/streams/rest.py:855`) returns `backoff.expo(factor=2)` — a generator that never inspects the failing exception or its response. I verified the consumption mechanism by reading `backoff` 2.3.1 internals: `backoff.on_exception` drives the wait generator with the **send protocol** — `_common._init_wait_gen` primes it with `.send(None)`, then `_next_wait` calls `wait.send(exception)` on each retry. So the exception (carrying `RetriableAPIError.response` and its headers) is *already delivered into the generator*; the default simply discards it. That discarded value is the root cause — header timing is structurally unreachable.

### Proposed Solution

Make the default `backoff_wait_generator` header-aware via the existing `backoff_runtime` send-protocol hook, while preserving full back-compat (the method stays overridable; taps that already override it are unaffected). Concretely:

1. Add an overridable **`get_wait_time_from_response(response) -> float | None`** on `_HTTPStream`: read `Retry-After` (integer seconds **or** HTTP-date → delta), else `X-RateLimit-Reset` (delta-seconds per the IETF draft); clamp negatives/zero to `0.0`; return `None` on any parse failure (never raise).
2. Add a private **`_parse_retry_after(value)`** helper for the dual numeric / HTTP-date format (`email.utils.parsedate_to_datetime`).
3. Rewrite `backoff_wait_generator` to yield the header-derived wait when present, otherwise fall through to a **primed, stateful** `backoff.expo(factor=2)` so missing-header retries still advance `2, 4, 8 …`.

**Defensive details discovered in Phase II:**
- Use `getattr(exc, "response", None)` — `RetriableAPIError.response` defaults to `None`, and other retriable exceptions in the decorator tuple (`ConnectionError`, `Timeout`, `ChunkedEncodingError`, …) have **no** `.response`.
- Do **not** change `backoff_jitter`; the default `random_jitter` *adds* `~[0,1)s` to the honored wait. Tests therefore assert the generator's yielded value (pre-jitter), not slept wall-clock time.

### Files I Will Touch (Phase III)

| File | Change |
|---|---|
| `singer_sdk/streams/rest.py` | Add `get_wait_time_from_response` + `_parse_retry_after` on `_HTTPStream`; rewrite `backoff_wait_generator` (~L855). |
| `tests/core/rest/test_failure.py` | New unit + generator-level tests (see Testing Strategy). |
| `docs/code_samples.md` | Document that header-respect is now the default and how to override/disable. |

### How I Will Verify

- `cd fork/sdk && uv run pytest tests/core/rest/test_failure.py -q` (targeted), then `nox -s tests` (full suite) + `nox -t typing` + `pre-commit run --all`.
- The Phase II reproduction, flipped into a passing assertion, becomes the headline regression test.

### Open Question (to resolve in Phase III)

`X-RateLimit-Reset` semantics: IETF draft defines it as *delta-seconds*, but some APIs send a *Unix epoch*. Default to delta-seconds (matching PR #3672); confirm the intended interpretation with a maintainer.

---

## Testing Strategy

All implemented in `tests/core/rest/test_failure.py`. Tests assert the **generator's yielded value / helper output (pre-jitter)**, never slept wall-clock time, because the default `random_jitter` adds `~[0,1)s`.

### Unit Tests (`test_get_wait_time_from_response*`)

- [x] **Test 1:** `Retry-After: 15` (integer string) → `15.0`.
- [x] **Test 2:** `Retry-After: <HTTP-date>` under a frozen clock (`time_machine`) → correct remaining delta seconds (`test_get_wait_time_from_response_http_date`).
- [x] **Test 3:** `X-RateLimit-Reset: 42` with no `Retry-After` → `42.0`.
- [x] **Test 4:** `Retry-After` takes precedence over `X-RateLimit-Reset` when both present.
- [x] **Test 5:** Negative reset value → clamped to `0.0`.
- [x] **Test 6:** Unparsable / missing headers → `None` (parametrized cases), and the generator falls back to exponential `2, 4, 8 …` without raising (`test_backoff_wait_generator_falls_back_to_exponential`).
- [x] **Test 7:** Exception with no `.response` (`ConnectionError`) → graceful exponential fallback.

### Generator-Level / Integration Tests

- [x] **Scenario 1:** Drive the real send protocol — `test_backoff_wait_generator_respects_headers` primes with `send(None)` then sends `RetriableAPIError`s, asserting `15.0` / `42.0` / precedence; `…falls_back_to_exponential` confirms state is preserved across header-less retries (`2`, then `4`, then `8`).
- [x] **Scenario 2:** Full `pytest tests/core` → **626 passed, 1 xfailed**; the existing `backoff.constant(0)` override tests in `test_authenticators.py` still pass, confirming no regression and that the method remains overridable.

### Manual Testing

Verified the end-to-end behavior interactively against the cloned SDK: a default stream's `backoff_wait_generator` returns `15.0` for `Retry-After: 15`, `42.0` for `X-RateLimit-Reset: 42`, `7.0` when both are present, and the exponential sequence `2, 4, 8` when no headers are present.

---

## Implementation Notes

### Week 1 Progress

Successfully finalized Phase I milestones. Evaluated prospective code targets across multiple repositories and locked down the Meltano Singer SDK framework enhancement. Traced the root cause directly to the `backoff_wait_generator` engine in the REST module, verified that it can be tested completely offline via mock assertions with zero compute constraints, and submitted the required portfolio architecture.

### Week 2 Progress (Phase II — Reproduce & Plan)

Cloned the upstream SDK, stood up the `uv`/`nox` dev environment (installing the missing toolchain), and ran the targeted REST failure tests green. Reproduced the shortcoming deterministically with an offline script (run twice, identical output), proving `backoff_wait_generator` is header-blind. Then did a deep read of the source and the `backoff` 2.3.1 internals — which corrected my initial plan in several ways: the methods live on `_HTTPStream` (not `RESTStream`), the integration point is the `.send()` protocol via `backoff_runtime`, `RetriableAPIError.response` can be `None`, and default jitter adds to the honored wait so tests must assert pre-jitter values. Finalized a verified, mutable UMPIRE plan. No solution code written (Phase II is reproduce + plan only).

### Week 3 Progress (Phase III — Build)

**What I built** (branch [`fix-issue-2012`](https://github.com/Rizsaurav/sdk/tree/fix-issue-2012) on my fork, rebased onto latest upstream `main`):

- `singer_sdk/streams/rest.py`:
  - Added `get_wait_time_from_response(response)` on `_HTTPStream` — reads `Retry-After` (delay-seconds **or** HTTP-date form) and falls back to `X-RateLimit-Reset` (seconds-until-reset); clamps to non-negative; returns `None` when no usable header is present. Overridable for non-standard headers.
  - Added a static `_parse_retry_after(value)` helper using `email.utils.parsedate_to_datetime` for the dual numeric / HTTP-date format.
  - Rewrote `backoff_wait_generator` to honor the header wait via the existing `backoff_runtime` `.send()` hook, falling through to a **primed, stateful** `backoff.expo(factor=2)` so header-less retries still advance `2, 4, 8 …`. Changed its send-type annotation to `t.Generator[float, t.Any, None]` to match the real send-protocol (and `backoff.expo`'s own type).
- `tests/core/rest/test_failure.py`: 4 new test functions (11 cases incl. parametrization) — see Testing Strategy.
- `docs/code_samples.md`: documented that header-respect is now the default and how to override `get_wait_time_from_response`.

**Validation:** `ruff check` + `ruff format` clean, `mypy` clean on changed files, full `pre-commit run --files …` green, and `pytest tests/core` → **626 passed, 1 xfailed** (REST subset: 91 passed). No behavior change for taps that already override backoff (verified: existing `backoff.constant(0)` override tests still pass).

**Challenges faced:**

- *`mypy` rejected `generator.send(exc)` in tests* — the generator's send-type was annotated `None`. Rather than patch the tests with `# type: ignore`, I traced that `backoff.expo` itself is typed `Generator[float, Any, None]` and that backoff genuinely sends the exception in; corrected the source annotation to `Any`, which is both accurate and keeps the public override contract working.
- *`ruff` flagged a duplicate `datetime` import* — it was already imported in a `TYPE_CHECKING` block; since I now use it at runtime I moved it to the runtime imports.
- *`ruff` float-equality lint* — switched exact float assertions to `pytest.approx`.

**Commits this week** (on `fix-issue-2012`):

- `6fcf1c3` — feat: respect rate-limit headers in default REST backoff
- `48a59ca` — test: cover rate-limit header backoff behavior
- `8131ce3` — docs: document default rate-limit header backoff

---

## Code Changes

| Field | Details |
|---|---|
| **Branch** | [`fix-issue-2012`](https://github.com/Rizsaurav/sdk/tree/fix-issue-2012) on [Rizsaurav/sdk](https://github.com/Rizsaurav/sdk) |
| **Files modified** | `singer_sdk/streams/rest.py` (+ `get_wait_time_from_response`, `_parse_retry_after`, rewritten `backoff_wait_generator`); `tests/core/rest/test_failure.py` (4 new tests); `docs/code_samples.md` |
| **Key commits** | `6fcf1c3` feat (implementation), `48a59ca` test, `8131ce3` docs |
| **Approach decisions** | Built the fix on the SDK's own `backoff_runtime` `.send()` hook rather than re-plumbing the decorator; `Retry-After` takes precedence over `X-RateLimit-Reset`; `X-RateLimit-Reset` interpreted as delta-seconds (IETF draft); kept default jitter unchanged. An independent solution from the open upstream PR #3672 — to be reconciled before a PR is opened. |

---

## Pull Request

| Field | Details |
|---|---|
| **PR Link** | *(Pending Phase IV)* |
| **PR Description** | *(Pending Phase IV Draft)* |
| **Maintainer Feedback** | *(Pending Phase IV)* |
| **Status** | In Progress |

---

## Learnings & Reflections

### Technical Skills Gained

*(Pending Phase IV Evaluation)*

### Challenges Overcome

*(Pending Phase IV Evaluation)*

### What I'd Do Differently Next Time

*(Pending Phase IV Evaluation)*

---

## Resources Used

- [Meltano Singer SDK REST Developer Reference](https://sdk.meltano.com/en/latest/stream_maps.html)
- [MDN Web Docs: HTTP Retry-After Specification](https://developer.mozilla.org/en-US/docs/Web/HTTP/Headers/Retry-After)
- [Python Backoff Library Documentation & Wait Generators](https://pypi.org/project/backoff/)
