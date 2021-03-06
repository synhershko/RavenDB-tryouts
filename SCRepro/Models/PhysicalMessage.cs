﻿using System.Collections.Generic;
using NServiceBus;

namespace SCRepro.Models
{
    public class PhysicalMessage
    {
        public PhysicalMessage(TransportMessage message)
        {
            MessageId = message.Id;
            Headers = message.Headers;
            Body = message.Body;
            ReplyToAddress = message.ReplyToAddress.ToString();
            CorrelationId = message.CorrelationId;
            Recoverable = message.Recoverable;
            MessageIntent = message.MessageIntent;
        }



        public string MessageId { get; set; }

        public byte[] Body { get; set; }

        public Dictionary<string, string> Headers { get; set; }
        public string ReplyToAddress { get; set; }
        public string CorrelationId { get; set; }
        public bool Recoverable { get; set; }
        public MessageIntentEnum MessageIntent { get; set; }
    }
}
