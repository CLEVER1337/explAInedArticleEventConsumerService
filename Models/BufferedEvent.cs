public sealed record BufferedEvent(string Topic, int Partition, long Offset, string Payload);
