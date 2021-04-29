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
        private readonly Dictionary<string, List<TimeSpan>> responseTimesByAddress = new();

        public RoundRobinClusterClient(string[] replicaAddresses)
            : base(replicaAddresses)
        {
        }

        public override async Task<string> ProcessRequestAsync(string query, TimeSpan timeout)
        {
            var addressByTask = new Dictionary<Task, string>();
            var remainingTimeout = timeout;
            lock (responseTimesByAddress)
            {
                ReplicaAddresses = ReplicaAddresses
                    .OrderBy(addr =>
                    {
                        if (!responseTimesByAddress.ContainsKey(addr))
                            return 0;
                        return responseTimesByAddress[addr].Select(ts => ts.Milliseconds).Average();
                    }).ToArray();
            }
            for (var i = 0; i < ReplicaAddresses.Length; i++)
            {
                var uri = ReplicaAddresses[i];
                var webRequest = CreateRequest(uri + "?query=" + query);
                Log.InfoFormat($"Processing {webRequest.RequestUri}");

                var sw = new Stopwatch();
                sw.Start();

                var processTask = ProcessRequestAsync(webRequest);
                addressByTask[processTask] = uri;

                var timeoutForTask = remainingTimeout / (ReplicaAddresses.Length - i);
                var delayTask = Task.Delay(timeoutForTask);
                await Task.WhenAny(processTask, delayTask);

                if (processTask.IsCompleted)
                {
                    if (processTask.IsFaulted)
                        continue;
                    lock (responseTimesByAddress)
                    {
                        if (!responseTimesByAddress.ContainsKey(addressByTask[processTask]))
                            responseTimesByAddress[addressByTask[processTask]] = new List<TimeSpan>();
                        responseTimesByAddress[addressByTask[processTask]].Add(sw.Elapsed);
                    }
                    return processTask.Result;
                }
                remainingTimeout -= timeoutForTask;
            }

            throw new TimeoutException();
        }

        protected override ILog Log => LogManager.GetLogger(typeof(RandomClusterClient));
    }
}
