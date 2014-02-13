using System;

namespace SCRepro.Models
{
    public class Heartbeat
    {
        public Guid Id { get; set; }
        public DateTime LastReportAt { get; set; }
        public EndpointDetails OriginatingEndpoint { get; set; }
        public Status ReportedStatus { get; set; }
        public string KnownEndpointId { get; set; }
    }

    public enum Status
    {
        Beating,
        Dead
    }
}
