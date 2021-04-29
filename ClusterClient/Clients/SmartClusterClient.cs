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
        private readonly Dictionary<string, TimeStat> responseTimesByAddress = new();
        public SmartClusterClient(string[] replicaAddresses)
            : base(replicaAddresses)
        {
        }

        public override async Task<string> ProcessRequestAsync(string query, TimeSpan timeout)
        {
            var addressByTask = new Dictionary<Task, string>();
            var processRequestTasks = new List<Task>();
            var stopwatchForTask = new Dictionary<Task, Stopwatch>();
            var exceptions = new List<Exception>();
            var remainingTimeout = timeout;
            var totalSw = Stopwatch.StartNew();
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

                var timeoutForTask = (timeout - totalSw.Elapsed) / (ReplicaAddresses.Length - i);

                var processTask = ProcessRequestAsync(webRequest);
                stopwatchForTask[processTask] = Stopwatch.StartNew();
                addressByTask[processTask] = uri;

                processRequestTasks.Add(processTask);
                var delayTask = Task.Delay(timeoutForTask);

                var completedTask = Task.WhenAny(processRequestTasks);
                await Task.WhenAny(completedTask, delayTask);

                if (delayTask.IsCompleted)
                {
                    remainingTimeout -= timeoutForTask;
                    continue;
                }

                if (!completedTask.Result.IsCompletedSuccessfully)
                {
                    processRequestTasks.Remove(completedTask.Result);
                    exceptions.Add(completedTask.Result.Exception);
                    lock (responseTimesByAddress)
                    {
                        if (!responseTimesByAddress.ContainsKey(addressByTask[completedTask.Result]))
                            responseTimesByAddress[addressByTask[completedTask.Result]] = new TimeStat();
                        responseTimesByAddress[addressByTask[completedTask.Result]].Add(
                            timeout * 2);
                    }
                }
                else
                {
                    lock (responseTimesByAddress)
                    {
                        if (!responseTimesByAddress.ContainsKey(addressByTask[completedTask.Result]))
                            responseTimesByAddress[addressByTask[completedTask.Result]] = new TimeStat();
                        responseTimesByAddress[addressByTask[completedTask.Result]].Add(
                            stopwatchForTask[completedTask.Result].Elapsed);
                    }

                    return ((Task<string>)completedTask.Result).Result;
                }
            }
            if (totalSw.Elapsed < timeout - Epsilon)
                throw new AggregateException(exceptions);
            throw new TimeoutException();
        }

        protected override ILog Log => LogManager.GetLogger(typeof(RandomClusterClient));
    }
}
