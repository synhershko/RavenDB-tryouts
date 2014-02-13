using System;
using System.Collections.Generic;
using NServiceBus;

namespace SCRepro.Models
{
    public class ImportMessage : IMessage
    {
        protected ImportMessage(TransportMessage message)
        {
            UniqueMessageId = Guid.NewGuid().ToString(); //message.UniqueId();
            MessageId = message.Id;

            PhysicalMessage = new PhysicalMessage(message);

            Metadata = new Dictionary<string, object>
            {
                {"MessageId", message.Id},
                {"MessageIntent", message.MessageIntent},
                {"HeadersForSearching", string.Join(" ",message.Headers.Values)}
            };

            //add basic message metadata
        }

        public string UniqueMessageId { get; set; }
        public string MessageId { get; set; }

        public PhysicalMessage PhysicalMessage { get; set; }

        public Dictionary<string, object> Metadata { get; set; }
    }
}
