using System.Linq;
using log4net;
using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;

namespace ClusterClient.Clients
{
    public class ParallelClusterClient : ClusterClientBase
    {

        public ParallelClusterClient(string[] replicaAddresses)
            : base(replicaAddresses)
        {
        }

        public override async Task<string> ProcessRequestAsync(string query, TimeSpan timeout)
        {
            var processRequestTasks = new List<Task>();

            foreach (var uri in ReplicaAddresses)
            {
                var webRequest = CreateRequest(uri + "?query=" + query);
                Log.InfoFormat($"Processing {webRequest.RequestUri}");
                processRequestTasks.Add(ProcessRequestAsync(webRequest));
            }

            var delayTask = Task.Delay(timeout);
            while (processRequestTasks.Count != 0)
            {
                await Task.WhenAny(Task.WhenAny(processRequestTasks), delayTask);

                if (delayTask.IsCompleted)
                    throw new TimeoutException();

                var completedTask = processRequestTasks.First(t => t.IsCompleted);

                if (completedTask.IsFaulted)
                    processRequestTasks.Remove(completedTask);
                else
                    return ((Task<string>) completedTask).Result;
            }
            throw new TimeoutException();
        }

        protected override ILog Log => LogManager.GetLogger(typeof(RandomClusterClient));
    }
}
