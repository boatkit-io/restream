# Restream data streams

Restream data streams split one logical system into two physical paths:

- The existing authenticated Restream connection is the control plane. A web
  subscription allocates a viewer endpoint lease, and its disconnect or
  unsubscribe releases that lease. Cloud implementations refcount leases and
  forward only aggregate source activation to the device.
- A separate endpoint carries high-bandwidth `Envelope` records. It must use
  bounded memory and must never block ordinary Restream state, RPC, or event
  traffic.

There are two payload types:

- `Frame` is one atomically delivered whole frame. A frame may depend on earlier
  frames; `FlagRecovery` marks an independently consumable recovery point.
  Encoded video access units, sonar pings, and short independently timed audio
  blocks fit this type.
- `BlockSet` incrementally updates an indexed array frame such as radar spokes.
  Data blocks are provisional. A payload-free commit record publishes the frame
  only when the receiver has every item. Incomplete BlockSets are never recovery
  state.

When congested, a producer can discard provisional BlockSet work and send the
newest complete sweep as a recovery `Frame`. Generations and discontinuity
flags prevent stale data from an old producer session from being combined with
a new one. Once the bounded scheduler drops any atomic unit, it rejects all
dependent frames and BlockSet records with `ErrNeedsRecovery` until it accepts
a `Frame` carrying both `FlagRecovery` and `FlagDiscontinuity`. That recovery
frame supersedes any older queued work for the stream.
