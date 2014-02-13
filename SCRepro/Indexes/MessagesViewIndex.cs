﻿using System;
using System.Linq;
using Raven.Abstractions.Indexing;
using Raven.Client.Indexes;
using SCRepro.Models;

namespace SCRepro.Indexes
{
    public class MessagesViewIndex : AbstractMultiMapIndexCreationTask<MessagesViewIndex.SortAndFilterOptions>
    {
        public class SortAndFilterOptions
        {
            public string MessageId { get; set; }
            public string MessageType { get; set; }
            public bool IsSystemMessage { get; set; }
            public MessageStatus Status { get; set; }
            public DateTime ProcessedAt { get; set; }
            public string ReceivingEndpointName { get; set; }
            public TimeSpan? CriticalTime { get; set; }
            public TimeSpan? ProcessingTime { get; set; }
            public TimeSpan? DeliveryTime { get; set; }
            public string ConversationId { get; set; }
            public string[] Query { get; set; }
            public DateTime TimeSent { get; set; }
        }

        public MessagesViewIndex()
        {
            AddMap<ProcessedMessage>(messages => from message in messages
                                                 let resolved = LoadDocument<FailedMessage>("FailedMessages/" + message.UniqueMessageId)
                                                 select new SortAndFilterOptions
                                                 {
                                                     MessageId = (string)message.MessageMetadata["MessageId"],
                                                     MessageType = (string)message.MessageMetadata["MessageType"],
                                                     IsSystemMessage = (bool)message.MessageMetadata["IsSystemMessage"],
                                                     Status = resolved == null ? MessageStatus.Successful : MessageStatus.ResolvedSuccessfully,
                                                     TimeSent = (DateTime)message.MessageMetadata["TimeSent"],
                                                     ProcessedAt = message.ProcessedAt,
                                                     ReceivingEndpointName = ((EndpointDetails)message.MessageMetadata["ReceivingEndpoint"]).Name,
                                                     CriticalTime = (TimeSpan)message.MessageMetadata["CriticalTime"],
                                                     ProcessingTime = (TimeSpan)message.MessageMetadata["ProcessingTime"],
                                                     DeliveryTime = (TimeSpan)message.MessageMetadata["DeliveryTime"],
                                                     Query = message.MessageMetadata.Select(_ => _.Value.ToString()).ToArray(),
                                                     ConversationId = (string)message.MessageMetadata["ConversationId"],
                                                 });

            AddMap<FailedMessage>(messages => from message in messages
                                              where message.Status != FailedMessageStatus.Resolved
                                              let last = message.ProcessingAttempts.Last()
                                              select new
                                              {
                                                  MessageId = (object)last.MessageId,
                                                  MessageType = last.MessageMetadata["MessageType"],
                                                  IsSystemMessage = last.MessageMetadata["IsSystemMessage"],
                                                  Status = message.Status == FailedMessageStatus.Archived
                                                      ? MessageStatus.ArchivedFailure
                                                      : message.ProcessingAttempts.Count == 1
                                                          ? MessageStatus.Failed
                                                          : MessageStatus.RepeatedFailure,
                                                  TimeSent = (DateTime)last.MessageMetadata["TimeSent"],
                                                  ProcessedAt = last.AttemptedAt,
                                                  ReceivingEndpointName = ((EndpointDetails)last.MessageMetadata["ReceivingEndpoint"]).Name,
                                                  CriticalTime = (object)null,
                                                  ProcessingTime = (object)null,
                                                  DeliveryTime = (object)null,
                                                  Query = last.MessageMetadata.Select(_ => _.Value.ToString()),
                                                  ConversationId = last.MessageMetadata["ConversationId"],
                                              });

            Index(x => x.Query, FieldIndexing.Analyzed);
        }
    }
}