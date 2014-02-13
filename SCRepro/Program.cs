using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Raven.Abstractions.Indexing;
using Raven.Client.Embedded;
using SCRepro.Indexes;
using SCRepro.Models;

namespace SCRepro
{
    class Program
    {
        static long count = 0;

        static void Main(string[] args)
        {
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
					}
				});

                var documentsPerThread = 10000;
                Console.WriteLine("Pushing documents...");

                var stopwatch = new Stopwatch();
                stopwatch.Start();

                Parallel.For(0, 10, l =>
                                          {
                                              Interlocked.Increment(ref count);

                                              for (int i = 0; i < documentsPerThread; i++)
                                              {
                                                  using (var session = documentStore.OpenSession())
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
                                                      session.SaveChanges();
                                                  }
                                              }

                                              Interlocked.Decrement(ref count);
                                          });

                Console.WriteLine("Finished pushing documents, waiting for indexing to complete...");
                stopwatch.Stop();
                Console.WriteLine("Time to push {0} docs: {1}ms ({2} docs per sec on avg)", documentsPerThread * 10, stopwatch.ElapsedMilliseconds, (documentsPerThread * 10) / (stopwatch.ElapsedMilliseconds / 1000));
                //Console.ReadKey();

                var stats = documentStore.DatabaseCommands.GetStatistics();
                while (stats.StaleIndexes.Length > 0)
                {
                    Console.WriteLine("{0} Stale indexes: {1}", DateTime.UtcNow, stats.StaleIndexes.Length);
                    Console.WriteLine("{0} CurrentNumberOfItemsToIndexInSingleBatch: {1}", DateTime.UtcNow, stats.CurrentNumberOfItemsToIndexInSingleBatch);
                    Thread.Sleep(1000);
                    stats = documentStore.DatabaseCommands.GetStatistics();
                }

                Console.WriteLine("Indexing complete");

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
