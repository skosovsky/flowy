# Frozen acceptance requirements

Baseline: 8ad714f3954c4773a31aa9d723fff28c07673ef5. Denominator: 18; PASS only, partial/NOT RUN/FAIL count zero. Review the same final candidate tree. No release/consumer/dependency changes.

1. Document admission/drop contract before implementation.
2. Full 32-slot buffer drops before state/effect cloning; bounded nonblocking delivery.
3. Synchronous producer detachment and mutation isolation for accepted mutable state.
4. Synchronous producer detachment and mutation isolation for accepted mutable effects.
5. 1024/no-drain regression: 32 delivered, exactly 32 state clones.
6. State-only, effect-only, both, nil cloners and HasEffect=false covered; effect clones exactly accepted HasEffect events.
7. Concurrent drain covered, preserving ownership and bounded delivery.
8. Stop and event-context cancel before admission and during cloning covered, preserving rejection.
9. WaitResult authority/no drain and missing terminal event preserved.
10. Same 64-KiB audit fixture retained, 256/1024 cloned candidate <=2,300,000 B/op and <=64 allocs/op each sample.
11. Baseline/candidate >=3 samples on same Go/machine; isolated baseline probe, GOWORK=off, writable GOCACHE, no simultaneous heavy load.
12. ExecuteNode controls retain 0 B/op and 0 allocs/op.
13. OrdinaryStreaming <=7300 B/op and <=52 allocs/op, original fixture unchanged.
14. Timing >=3 samples; >10% median regression triggers longer >=3-sample comparison and benchstat assessment before decision.
15. New regressions and existing mutable ownership, WaitResult/no-drain and cancellation/cleanup tests pass under race.
16. Root contract suite PASS, goleak TestMain unchanged.
17. Durable encoding/history, host ownership, public API, dependencies and scope unchanged; no unbounded queue or consumer clone.
18. Two independent reviewers: A 18/18 = 100% APPROVE; B APPROVE with zero found unfixed errors on final revision; repeat after fixes.
