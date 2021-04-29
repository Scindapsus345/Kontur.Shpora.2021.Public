using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Threading.Tasks;
using log4net;

namespace ClusterClient.Clients
{
    public class RoundRobinClusterClient : ClusterClientBase
    {
        private readonly Dictionary<string, TimeStat> responseTimesByAddress = new();

        public RoundRobinClusterClient(string[] replicaAddresses)
            : base(replicaAddresses)
        {
        }

        public override async Task<string> ProcessRequestAsync(string query, TimeSpan timeout)
        {
            var totalSw = Stopwatch.StartNew();
            var remainingTimeout = timeout;
            string[] orderedReplicaAddresses;
            lock (responseTimesByAddress)
            {
                orderedReplicaAddresses = ReplicaAddresses
                    .OrderBy(addr =>
                    {
                        if (!responseTimesByAddress.ContainsKey(addr))
                            return 0;
                        return responseTimesByAddress[addr].AverageMilliseconds();
                    }).ToArray();
            }
            for (var i = 0; i < orderedReplicaAddresses.Length; i++)
            {
                var uri = ReplicaAddresses[i];
                var webRequest = CreateRequest(uri + "?query=" + query);
                Log.InfoFormat($"Processing {webRequest.RequestUri}");

                var sw = new Stopwatch();
                sw.Start();

                var processTask = ProcessRequestAsync(webRequest);

                var timeoutForTask = remainingTimeout / (ReplicaAddresses.Length - i);
                var delayTask = Task.Delay(timeoutForTask);
                await Task.WhenAny(processTask, delayTask);

                if (processTask.IsCompleted)
                {
                    if (!processTask.IsCompletedSuccessfully)
                        lock (responseTimesByAddress)
                        {
                            if (!responseTimesByAddress.ContainsKey(uri))
                                responseTimesByAddress[uri] = new TimeStat();
                            responseTimesByAddress[uri].Add(timeout * 2);
                        }
                    else
                    {
                        lock (responseTimesByAddress)
                        {
                            if (!responseTimesByAddress.ContainsKey(uri))
                                responseTimesByAddress[uri] = new TimeStat();
                            responseTimesByAddress[uri].Add(sw.Elapsed);
                        }
                        return processTask.Result;
                    }
                }
                remainingTimeout -= sw.Elapsed;
            }
            if (totalSw.Elapsed < timeout)
                throw new Exception();
            throw new TimeoutException();
        }

        protected override ILog Log => LogManager.GetLogger(typeof(RandomClusterClient));
    }
}
