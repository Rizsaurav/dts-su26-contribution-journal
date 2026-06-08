# DTS SU26 Contribution Journal

---

## Contribution 1: `feat: Respect 'RateLimit' headers in default REST backoff implementation`

| Field | Details |
|---|---|
| **Contribution Number** | 1 |
| **Student** | Saurav Rijal |
| **Issue** | [meltano/sdk#2143](https://github.com/meltano/sdk/issues/2143) |
| **Status** | Phase I Complete |

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

The core components involved are located within the REST stream orchestration module of the SDK framework, specifically:

- `singer_sdk/streams/rest.py` — specifically `RESTStream.request_decorator`, `backoff_wait_generator`, and `backoff_runtime`

---

## Reproduction Process

### Environment Setup

> Pending Phase II: Local Python virtual environment setup, package editing installation (`pip install -e ".[testing]"`), and initial `pytest` tracing will execute during Phase II setup.

### Steps to Reproduce

1. Initialize a baseline REST stream component utilizing the default SDK configuration.
2. Trigger an API request that yields a simulated HTTP `429` exception containing an explicit header rule (e.g., `Retry-After: 15`).
3. **Observed result:** The system overrides or bypasses the explicit 15-second metric, opting instead to execute standard base exponential scaling intervals.

### Reproduction Evidence

- **Commit showing reproduction:** *(Pending Phase II)*
- **Screenshots/logs:** *(Pending Phase II)*
- **My findings:** *(Pending Phase II)*

---

## Solution Approach

### Analysis

The root cause resides in the abstract structural binding within `singer_sdk/streams/rest.py`. The `request_decorator` dynamically hooks into a static configuration array that supplies `self.backoff_wait_generator` directly to the underlying `backoff.on_exception` wrapper. Because this generator defaults implicitly to an exponential schema without inspecting the contextual response payload details bound to `RetriableAPIError`, header parsing is entirely skipped at runtime.

### Proposed Solution

I propose upgrading `backoff_wait_generator` inside `RESTStream` to dynamically resolve wait delays. By validating if the caught exception encapsulates a valid HTTP response with rate-limiting attributes, the application can extract integer seconds or compile Delta-T variables from standard GMT datetime strings. If verified, it yields that precise duration; otherwise, it passes execution cleanly back to the traditional `backoff.expo` generator.

### Implementation Plan

Using the UMPIRE framework (adapted):

- **Understand:** The default REST client requires native support for reading HTTP rate-limit windows directly from error payloads to avoid blind backoff execution.
- **Match:** Investigate how other robust Python integration frameworks or custom Meltano sub-taps parse HTTP dictionary headers safely without risking missing-key crashes.
- **Plan:**
  1. Navigate to `singer_sdk/streams/rest.py` and inspect `backoff_wait_generator`.
  2. Construct an internal helper utility designed to parse strings, epoch timestamps, and raw digits out of `Retry-After`, `X-RateLimit-Reset`, and related fields.
  3. Integrate this evaluation step upstream of the fallback exponential generator.
  4. Update unit test frameworks to ensure comprehensive coverage.
- **Implement:** *(Pending Phase III)*
- **Review:** Ensure the structural alterations strictly observe Meltano's architectural conventions, typehint boundaries, and linting baselines.
- **Evaluate:** Execute mock assertions demonstrating that a stream correctly pauses for the exact duration specified by a mock header response.

---

## Testing Strategy

### Unit Tests

- [ ] **Test 1:** Validate accurate conversion of standard integer string headers (e.g., `Retry-After: 12`) directly into float second parameters.
- [ ] **Test 2:** Validate the parsing of HTTP-date string structures (e.g., `Retry-After: Fri, 31 Dec 2027 23:59:59 GMT`) into correct remaining delta seconds relative to system time.
- [ ] **Test 3:** Confirm that missing or corrupted headers seamlessly fall back to standard exponential backoff behavior without throwing unhandled exceptions.

### Integration Tests

- [ ] **Scenario 1:** Simulate a mock target pipeline connection running through successive `429` states to verify step-by-step backoff compliance.
- [ ] **Scenario 2:** Ensure that standard, successful (`200 OK`) request execution loops remain entirely unaffected by the added error-handling paths.

### Manual Testing

*(Pending Phase III/IV)*

---

## Implementation Notes

### Week 1 Progress

Successfully finalized Phase I milestones. Evaluated prospective code targets across multiple repositories and locked down the Meltano Singer SDK framework enhancement. Traced the root cause directly to the `backoff_wait_generator` engine in the REST module, verified that it can be tested completely offline via mock assertions with zero compute constraints, and submitted the required portfolio architecture.

### Week 2 Progress

*(Pending Phase II Progression)*

---

## Code Changes

| Field | Details |
|---|---|
| **Files modified** | *(Pending Phase III)* |
| **Key commits** | *(Pending Phase III)* |
| **Approach decisions** | *(Pending Phase III)* |

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
