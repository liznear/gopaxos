# gopaxos

This is a simple implementation of the Paxos algorithm in Go. It is
based on the
paper ["MultiPaxos Made Complete"](https://arxiv.org/html/2405.11183v1#S3.SS3).

## Example KV Store

This is a simple example of a key-value store that uses this Paxos
library. You can run it using `just run1`, `just run2`, and `just run3`
in separate terminals to start 3 nodes.

## TODO

- [ ] Make the `log` part customizable. We can probably create a `log`
  interface which can be implemented using memory, disk, etc.
- [ ] Support getting snapshot so that a node can recover from a crash
  by requesting this snapshot. This snapshot may need to be application
  specific.