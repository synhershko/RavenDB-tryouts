using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using metrics;
using metrics.Core;
using Raven.Abstractions.Indexing;
using Raven.Client.Embedded;
using SCRepro.Indexes;
using SCRepro.Models;

namespace SCRepro
{
    class Program
    {
        private static readonly MeterMetric indexedMeter = Metrics.Meter(typeof(ProcessedMessage), "messages", "indexed", TimeUnit.Seconds);

        static void Main(string[] args)
        {
            Metrics.EnableConsoleReporting(10, TimeUnit.Seconds);
            var dataDir = GetDbPath();
            //Directory.Delete(dataDir);

            // Initialize RavenDB
            using (
                var documentStore = new EmbeddableDocumentStore
                                    {
                                        DataDirectory = dataDir,
                                        UseEmbeddedHttpServer = true,
                                        EnlistInDistributedTransactions = false
                                    })
            {

                documentStore.Configuration.Port = 33333;
                documentStore.Configuration.HostName = "localhost";
                documentStore.Configuration.CompiledIndexCacheDirectory = dataDir;
                documentStore.Configuration.VirtualDirectory = "/storage";

                documentStore.Conventions.SaveEnumsAsIntegers = true;

                documentStore.Initialize();

                new MessagesViewIndex().Execute(documentStore);
                documentStore.DatabaseCommands.PutIndex("Raven/DocumentsByEntityName", new IndexDefinition
				{
					Map =
						@"from doc in docs 
let Tag = doc[""@metadata""][""Raven-Entity-Name""]
select new { Tag, LastModified = (DateTime)doc[""@metadata""][""Last-Modified""] };",
					Indexes =
					{
						{"Tag", FieldIndexing.NotAnalyzed},
						{"LastModified", FieldIndexing.NotAnalyzed},
					},
					Stores =
					{
						{"Tag", FieldStorage.No},
						{"LastModified", FieldStorage.No}
					},
					TermVectors =
					{
						{"Tag", FieldTermVector.No},
						{"LastModified", FieldTermVector.No}						
					},
                    DisableInMemoryIndexing = true,
				});

                const int documentsPerThread = 50000;
                const bool doBulk = true;
                const int bulkSize = 100;
                Console.WriteLine("Pushing documents...");
                if (doBulk)
                {
                    Console.WriteLine("(using bulk size of {0})", bulkSize);
                }

                var stopwatch = new Stopwatch();
                stopwatch.Start();

                Parallel.For(0, 10, l =>
                                    {
                                        using (var session = documentStore.BulkInsert())
                                        {
                                        for (int i = 0; i < documentsPerThread; i++)
                                              {
                                                      var msgGuid = Guid.NewGuid().ToString();
                                                      var msg = new ProcessedMessage
                                                            {
                                                                Id = "ProcessedMessage/" + msgGuid,
                                                                MessageMetadata = new Dictionary<string, object>
                                                                {
                                                                    {"MessageId", Guid.NewGuid().ToString()},
                                                                    {"MessageIntent", "1"},
                                                                    {"HeadersForSearching", "967cfa52-d114-4958-940a-a2d100053f8f 967cfa52-d114-4958-940a-a2d100053f8f MyClient USER-PC Send 4.3.4 2014-02-12 22:19:06:505614 Z 102ade9ea86c452da6394e4ba7e2900c text/xml MyMessages.RequestDataMessage, MyMessages, Version=1.0.0.0, Culture=neutral, PublicKeyToken=null 967cfa52-d114-4958-940a-a2d100053f8f\\0  316b2439-21b4-4b45-8989-a2d100053f8f MyServer USER-PC 2014-02-12 22:19:06:509614 Z 2014-02-12 22:19:06:510615 Z MyClient@USER-PC" + Guid.NewGuid()},
                                                                    {"TimeSent", DateTime.UtcNow},
                                                                    {"CriticalTime", TimeSpan.FromMinutes(1)},
                                                                    {"ProcessingTime", TimeSpan.FromMinutes(2)},
                                                                    {"DeliveryTime", TimeSpan.FromSeconds(2)},
                                                                    {"ContentLength", 213},
                                                                    {"ContentType", "text/xml"},
                                                                    {"SearchableBody", "<?xml version=\"1.0\" ?>\r\n<Messages xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\" xmlns:xsd=\"http://www.w3.org/2001/XMLSchema\" xmlns=\"http://tempuri.net/MyMessages\">\n<RequestDataMessage>\n<DataId>102ade9e-a86c-452d-a639-4e4ba7e2900c</DataId>\n<String>&lt;node&gt;it&apos;s my &quot;node&quot; &amp; i like it&lt;node&gt;</String>\n</RequestDataMessage>\n</Messages>\r\n"},
                                                                    {"BodyUrl", "/messages/6d004d6f-56c2-fe71-15d1-27199b59484b/body"},
                                                                    {"BodySize", 369},
                                                                    {"IsSystemMessage", false},
                                                                    {"MessageType", "MyMessages.RequestDataMessage"},
                                                                    {"SearchableMessageType", "MyMessages RequestDataMessage"},
                                                                    {"SendingEndpoint", new EndpointDetails {Name = "MyClient", Machine = "USER-PC"}},
                                                                    {"ReceivingEndpoint", new EndpointDetails {Name = "MyClient", Machine = "USER-PC"}},
                                                                    {"ConversationId", Guid.NewGuid().ToString()},
                                                                },
                                                                Headers = new Dictionary<string, string>
                                                                {
                                                                    {"NServiceBus.MessageId", msgGuid},
                                                                    {"NServiceBus.CorrelationId", msgGuid},
                                                                    {"NServiceBus.OriginatingEndpoint", "MyClient"},
                                                                    {"NServiceBus.OriginatingMachine", "USER-PC"},
                                                                    {"NServiceBus.MessageIntent", "Send"},
                                                                    {"NServiceBus.Version", "4.3.4"},
                                                                    {"NServiceBus.TimeSent", DateTime.UtcNow.ToString()},
                                                                },
                                                                ProcessedAt = DateTime.UtcNow,
                                                                UniqueMessageId = msgGuid,
                                                            };
                                                      session.Store(msg);
                                                      indexedMeter.Mark();
                                                  
                                                    }}
                                          });

                Console.WriteLine("Finished pushing documents, waiting for indexing to complete...");
                stopwatch.Stop();
                var elapsed = stopwatch.Elapsed;
                Console.WriteLine("Time to push {0} docs: {1} ({2} docs per sec on avg)", documentsPerThread * 10, elapsed, (documentsPerThread * 10) / (stopwatch.ElapsedMilliseconds / 1000));
                stopwatch = new Stopwatch();
                stopwatch.Start();

                var stats = documentStore.DatabaseCommands.GetStatistics();
                while (stats.StaleIndexes.Length > 0)
                {
                    Console.WriteLine("{0} Stale indexes: {1}", DateTime.UtcNow, stats.StaleIndexes.Length);
                    Console.WriteLine("{0} CurrentNumberOfItemsToIndexInSingleBatch: {1}", DateTime.UtcNow, stats.CurrentNumberOfItemsToIndexInSingleBatch);
                    Thread.Sleep(1000);
                    stats = documentStore.DatabaseCommands.GetStatistics();
                }
                stopwatch.Stop();

                Console.WriteLine("Indexing complete - waited {0} after the initial documents push (total of {1})", stopwatch.Elapsed, elapsed + stopwatch.Elapsed);
                Console.ReadKey();
            }
        }

        static string GetDbPath()
        {
            const string dbFolder = "sc-repro";
            return Path.Combine(Environment.GetFolderPath(Environment.SpecialFolder.CommonApplicationData),
                "Particular", "ServiceControl", dbFolder);
        }
    }
}
