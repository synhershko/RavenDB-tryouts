using System;

namespace SCRepro.Models
{
    public class FailureDetails
    {
        public string AddressOfFailingEndpoint { get; set; }

        public DateTime TimeOfFailure { get; set; }

        public ExceptionDetails Exception { get; set; }

    }

    public class ExceptionDetails
    {
        public string ExceptionType { get; set; }
        public string Message { get; set; }
        public string Source { get; set; }
        public string StackTrace { get; set; }
    }
}
