﻿using System;
using System.Collections.Generic;
using NServiceBus;

namespace SCRepro.Models
{
    public class ProcessedMessage
    {
        public ProcessedMessage()
        {
            MessageMetadata = new Dictionary<string, object>();
        }
        public ProcessedMessage(ImportSuccessfullyProcessedMessage message)
        {

            Id = "ProcessedMessages/" + message.UniqueMessageId;
            UniqueMessageId = message.UniqueMessageId;
            MessageMetadata = message.Metadata;
            Headers = message.PhysicalMessage.Headers;

            string processedAt;

            if (message.PhysicalMessage.Headers.TryGetValue(NServiceBus.Headers.ProcessingEnded, out processedAt))
            {
                ProcessedAt = DateTimeExtensions.ToUtcDateTime(processedAt);
            }
            else
            {
                ProcessedAt = DateTime.UtcNow;//best guess    
            }
        }
        public string Id { get; set; }

        public string UniqueMessageId { get; set; }

        public Dictionary<string, object> MessageMetadata { get; set; }

        public Dictionary<string, string> Headers { get; set; }

        public DateTime ProcessedAt { get; set; }
    }

    public class ImportSuccessfullyProcessedMessage : ImportMessage
    {
        public ImportSuccessfullyProcessedMessage(TransportMessage message)
            : base(message)
        {
        }
    }
}
