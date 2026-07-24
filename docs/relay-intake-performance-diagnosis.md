# Relay intake performance diagnosis

Date: 2026-07-24

## Scope

This investigation measures only relay intake: ChainSync headers, range
construction, BlockFetch bodies, queue pressure, network throughput, and host
resource pressure. Publication timing and capacity are deliberately excluded.

All comparisons used actual mainnet relay data, one N2N connection per relay,
strict agreement between two independently operated relays, and the committed
512-block BlockFetch range. No source change is made as part of this
diagnosis.

## Conclusion

The fall from roughly 550 to 250 blocks/second is not a local processing
regression. It is primarily a change in the selected relay/IP behavior
(remote service, Internet path, and mini-protocol scheduling), amplified by
changes in average block size.

The strongest controlled result used the same binary, configuration, warm-up,
and exact 32,768-block span:

| Relay pair | Observed blocks/s |
|---|---:|
| DNS-selected production IPs | 176.401 |
| Currently advertised, explicitly pinned fast IPs | 440.389 |

The fast pair was **2.50 times** as fast on identical blocks. The slow pair
delivered about 27.5-28.4 Mbit/s per relay; the fast pair delivered about
70-75 Mbit/s. BlockFetch body rate followed those byte rates.

There is some remaining ChainSync scheduling headroom. On a pinned fast pair,
refilling one `RequestNext` for every response repeatedly beat the committed
20-response refill behavior. The two batch-1 runs achieved 482.330 and
494.259 blocks/second; all four batch-20 controls were between 409.979 and
451.757 blocks/second. This is a credible candidate improvement, but it is
much smaller than the 2.50x endpoint/path effect and does not make a slow path
fast.

## Controlled method

The refill and endpoint trials used:

- start point
  `146287827:a2b8720137f5db20f00587d45ea187aa0047cb05e20bfacb76ce0d5fc2b64544:11400000`;
- a 16,384-block warm-up;
- the exact scored range 11,416,385 through 11,449,152;
- 32,768 scored blocks;
- fresh disposable ClickHouse volumes for every run;
- 512 blocks per BlockFetch range;
- the same queue and memory limits;
- one connection to each of two required relays;
- database `observed_at` timestamps, not insertion timestamps, for the scored
  rate.

Every scored run contained exactly 32,768 unique block numbers, block hashes,
publication IDs, and adoption event IDs. Each had 14.633636 transactions and
137.694794 selected facts per block, valid relay provenance, zero agreement
mismatches, zero reconnects, and one connection attempt.

The fixed block span matters. Comparing arbitrary live intervals confounds
code behavior with both public-network drift and block density.

## Endpoint/path comparison

The slow pair was selected by the normal relay hostnames during the degraded
production run:

- IOG: `3.77.115.8:3001`;
- Cardano Foundation: `2.24.118.152:3001`.

The currently advertised fast pair was pinned explicitly:

- IOG: `3.135.125.51:3001`;
- Cardano Foundation: `185.175.59.249:3001`.

| Pair and refill | Observed blocks/s | Raw ingress per relay | Process snapshot |
|---|---:|---:|---:|
| Slow pair, batch 20 | 176.401 | about 27.5-28.4 Mbit/s | 34% CPU, 320 MiB |
| Slow pair, batch 1 | 188.872 | about 29.5 Mbit/s | 44% CPU, 347 MiB |
| Fast pair, batch 20 | 440.389 | about 70-75 Mbit/s | 77% CPU, 594 MiB |

The batch-1 slow-pair result is 7.1% higher, but the rounded byte rate rose by
about the same amount. It is therefore not evidence that request scheduling
can overcome that path's bandwidth ceiling.

The production hostnames currently have multiple A records. Connection setup
selects a reachable address; it does not score addresses by sustained
BlockFetch throughput. A restart can consequently move the process between
very different paths. After this benchmark, restoring production selected
`35.177.184.223` and `185.175.59.249`; it initially delivered about
27-30 Mbit/s and 320-330 blocks/second on smaller blocks. This is consistent
with byte throughput divided by block size, not a stable per-hostname block
rate.

After six minutes, that restored pair exposed two different pressure shapes:

| Relay | Headers/s | Bodies/s | Fetch duty | Next range prepared | Inter-range idle |
|---|---:|---:|---:|---:|---:|
| IOG `35.177.184.223` | 298.4 | 298.3 | 77.8% | 0% | 377 ms |
| Foundation `185.175.59.249` | 301.4 | 298.6 | 99.4% | 99.1% | 2.9 ms |

The IOG worker was waiting for ChainSync headers between every range. The
Foundation worker was continuously fetching bodies. Strict agreement
therefore had approximately the same 298-block/second ceiling from different
causes on the two connections. This is why both endpoint behavior and
ChainSync scheduling must be inspected; a lifetime raw Mbit/s value alone
does not identify which protocol phase created idle time.

ICMP latency is not a useful selector here. The slow Foundation address was
about 40 ms away while the faster Foundation address was about 130 ms away.
The protocol pipeline hides much of the round-trip latency; sustained
single-flow body throughput is the relevant measurement.

## ChainSync refill comparison

The committed driver keeps at most 100 requests outstanding. Batch 20 does
not send 100 and wait for all responses: it refills from 80 back to 100 after
each 20 completed callbacks. Packet capture previously confirmed that these
refills occur throughout the response stream.

The refill experiment used a pinned fast pair:

- IOG: `18.221.168.221:3001`;
- Cardano Foundation: `185.175.59.249:3001`.

Runs were deliberately bracketed with batch-20 controls to expose public-path
drift:

| Run order | Refill batch | Observed blocks/s |
|---:|---:|---:|
| 1 | 20 | 437.814 |
| 2 | 5 | 421.068 |
| 3 | 20 | 409.979 |
| 4 | 1 | 494.259 |
| 5 | 20 | 420.219 |
| 6 | 1 | 482.330 |
| 7 | 20 | 451.757 |

Batch 5 was 0.6% below the geometric mean of its adjacent controls, so it
showed no measurable gain. The two batch-1 runs were 19.1% and 10.7% above
their respective adjacent-control geometric means. Even the slower batch-1
run beat the fastest batch-20 control by 6.8%.

The evidence supports trying batch 1 as the next clean code change. It does
not prove a universal percentage gain: the samples are short, public paths
still drift, and batch 1 performs roughly 20 times as many synchronous mux
writes as batch 20. The observed process remained below one CPU core, and the
extra framing volume is negligible, so those costs have substantial headroom.

## Pressure analysis

### ChainSync

ChainSync continuously maintains its sliding request window. Header delivery
matched or exceeded body delivery in the scored runs. The pending-header
buffer repeatedly contained hundreds of headers and reached its configured
512-header high-water mark.

Batch 20 still creates a 100-to-80 outstanding-request sawtooth. On fast
paths, some ranges were not ready before the prior BlockFetch stream ended.
The repeatable batch-1 result indicates that closing this small scheduling
bubble can help once relay bandwidth is high enough. Batch 5 did not produce
a measurable improvement, so the precise mechanism should be confirmed in a
longer batch-1 run rather than inferred solely from the window shape.

The restored production IOG connection is a direct header-starvation case:
its current header and body rates are equal, no next range is ready in
advance, and BlockFetch is idle about 22% of wall time. Batch-1 refill is the
correct narrow experiment for this condition. The other required relay is
already body-bound near the same overall rate, so eliminating IOG's idle
cannot raise strict agreement beyond the Foundation connection's current
capacity.

### Range construction

The one-slot fetch-job queue was normally empty or held exactly the next
range. A high enqueue-wait average on the faster relay means that the next
range was already prepared while the current range occupied the worker; it
does not mean work was lost.

The 512-block range remains the best measured setting. Previous actual-relay
trials with 2,048 and 8,000 blocks drained the header reserve, reduced
BlockFetch duty, and introduced multi-second inter-range gaps. An explicit
16,000-header prefill only delayed that depletion. A larger queue or initial
backlog does not change the steady-state header flow rate.

### BlockFetch

On the slow fixed-span pair, the two BlockFetch workers were active for about
93% and 99% of wall time. Their body rates were about 179-180 blocks/second,
matching the 176.401 agreed observation rate after small range-transition
costs. On the fixed fast paths, the limiting worker was generally about
91-96% active.

There was no meaningful raw-byte budget wait. Body callbacks and header rates
moved together, and there were no reconnects or protocol errors. BlockFetch
was therefore operating near the capacity supplied by each selected
connection. The remaining idle fraction is the only scheduling headroom that
batch-1 ChainSync refills can address.

This conclusion is per connection, not universal: the restored production
IOG worker is currently header-bound at 77.8% duty, while its Foundation peer
is body-bound at 99.4%. BlockFetch itself is full when it has a range; the IOG
worker simply does not receive complete ranges quickly enough.

### Strict agreement

The fastest relay sometimes filled a bounded event queue while waiting for
the slower required relay. That is expected: strict all-relay agreement is
necessarily limited by the slowest connection. Increasing capacity after the
faster relay cannot raise the agreed rate.

### Host and local network

During the 440.389-block/second run:

- Clicksync used 77% of one logical CPU;
- the host was 93% CPU-idle with 1% I/O wait;
- 14 GiB of memory was available and there was no active swap-in or swap-out;
- the NIC negotiated 1 Gbit/s full duplex;
- the NIC reported zero receive/transmit errors and zero drops;
- the two relay streams used roughly 140-150 Mbit/s in aggregate;
- TCP reported one retransmitted segment, no timeouts, and no fast/lost
  retransmits.

CPU saturation, memory pressure, local NIC capacity, and local packet loss are
all ruled out. This does not distinguish remote relay egress from congestion
elsewhere on the Internet path, but both are outside the local processing
pipeline.

## Reconciliation of 550 and 250 blocks/second

For a fixed span, the first-order relationship is:

```text
blocks/second = limiting relay bytes/second / average raw bytes/block
```

Here `relay bytes/second` is the wall-clock rate. It already includes
ChainSync starvation and inter-range gaps. Header rate, prepared-range ratio,
and fetch duty are needed to distinguish an underfed BlockFetch worker from a
body stream that is active nearly all the time.

Two earlier live intervals validate it:

| Interval | Raw bytes/block | Limiting ingress | Predicted | Observed |
|---|---:|---:|---:|---:|
| Faster sparse span | 14,741 | 64.39 Mbit/s | 546.00 blocks/s | about 545-550 |
| Denser span | 23,572 | 71.64 Mbit/s | 379.90 blocks/s | 380.17 |

For the controlled fixed span, about 27.53 Mbit/s and 19,216 bytes/block
predict 179.08 blocks/second, close to the measured 176.401. The production
logs around 223-250 blocks/second likewise showed only about 20-31 Mbit/s,
depending on the interval and selected addresses.

Thus both reported observations are real:

- roughly 550 blocks/second occurred on smaller blocks over a roughly
  64 Mbit/s limiting relay stream;
- roughly 250 blocks/second occurred when the selected paths supplied much
  less body bandwidth;
- neither number is a fixed capability of the binary.

## Recommended next actions

1. For controlled historical backfill, explicitly configure one
   benchmark-confirmed address per operator. At the time of this test,
   `3.135.125.51` and `185.175.59.249` were current DNS answers and delivered
   the 2.50x fixed-span improvement. Treat IPs as operational configuration,
   not permanent repository defaults; they can change or degrade.
2. Change the fixed ChainSync refill batch from 20 to 1 and repeat the same
   pinned-span benchmark. Do not add adaptive batching or another tuning
   subsystem.
3. Add the already captured negotiated remote address to the existing relay
   progress log. This makes a future path regression visible without querying
   stored provenance.
4. Keep one N2N connection per relay, a 100-request ChainSync maximum, and
   512-block BlockFetch ranges. Do not increase range/backlog size or add
   parallel connections.
5. If static operational pinning is unacceptable, design endpoint rotation
   separately. It should test candidates sequentially using raw Mbit/s and
   keep only one live connection per operator. RTT-based selection and
   simultaneous multi-connection racing are not supported by this evidence.

The batch-1 acceptance run should use actual relay data, bracketed batch-20
controls, and the same fixed span. It should retain zero reconnects,
mismatches, gaps, and provenance defects; keep the outstanding request count
at or below 100; and show that header readiness or inter-range idle improves.
No new synthetic performance test is needed.
