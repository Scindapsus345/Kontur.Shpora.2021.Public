using System.Linq;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Threading.Tasks;
using log4net;

namespace ClusterClient.Clients
{
    public class SmartClusterClient : ClusterClientBase
    {
        private readonly Dictionary<string, List<TimeSpan>> responseTimesByAddress = new();
        public SmartClusterClient(string[] replicaAddresses)
            : base(replicaAddresses)
        {
        }

        public override async Task<string> ProcessRequestAsync(string query, TimeSpan timeout)
        {
            var addressByTask = new Dictionary<Task, string>();
            var processRequestTasks = new List<Task>();
            var taskStartTime = new Dictionary<Task, DateTime>();
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

                var timeoutForTask = remainingTimeout / (ReplicaAddresses.Length - i);

                var processTask = ProcessRequestAsync(webRequest);
                taskStartTime[processTask] = DateTime.Now;
                addressByTask[processTask] = uri;

                processRequestTasks.Add(processTask);
                var delayTask = Task.Delay(timeoutForTask);

                await Task.WhenAny(Task.WhenAny(processRequestTasks), delayTask);

                if (delayTask.IsCompleted)
                {
                    remainingTimeout -= timeoutForTask;
                    continue;
                }
                var completedTask = processRequestTasks.First(t => t.IsCompleted);
                if (completedTask.IsFaulted)
                    processRequestTasks.Remove(completedTask);
                else
                {
                    lock (responseTimesByAddress)
                    {
                        if (!responseTimesByAddress.ContainsKey(addressByTask[completedTask]))
                            responseTimesByAddress[addressByTask[completedTask]] = new List<TimeSpan>();
                        responseTimesByAddress[addressByTask[completedTask]].Add(
                            DateTime.Now - taskStartTime[completedTask]);
                    }

                    return ((Task<string>) completedTask).Result;
                }
            }

            throw new TimeoutException();
        }

        protected override ILog Log => LogManager.GetLogger(typeof(RandomClusterClient));
    }
}
