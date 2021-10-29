Tests:

`cargo test`

Basic benchmark:

`cargo test test_bench -- --ignored --nocapture`

# Design Overview

this component takes batches of records and bundles them together for
bulk storage and querying.

- segments: individual batches of records in e.g. a single parquet file
- (s)log: a well-structured sequence of segments. each has a background
  writer thread responsible for committing full segments. load shedding
  occurs by dropping any records if the current active segment is full
  while a background write is pending.
- topic: a high-level group of records. can have any number of partitions,
  which are effectively individual slogs plus some metadata.
- topic state records segment data for individual partition slogs:
    - start / end indices
    - overlapping start/end time segments
    - segment sizes