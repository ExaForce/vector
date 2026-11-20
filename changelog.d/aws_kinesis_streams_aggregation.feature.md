Added optional record aggregation to the `aws_kinesis_streams` sink: many events are packed into one record as newline-delimited JSON and compressed together, measured at roughly 5x versus 2x on real CloudTrail data since Kinesis bills each record rounded up to 1 KB. Off by default; requires a compact `json` or `native_json` codec, replaces `partition_key_field` with a random key per record, and needs a consumer that splits on newlines. Also fixes both Kinesis sinks acking a partially failed batch as fully delivered, which dropped records that were never written.

authors: smolaon
