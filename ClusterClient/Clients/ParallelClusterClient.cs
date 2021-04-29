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
                var completedTask = Task.WhenAny(processRequestTasks);
                await Task.WhenAny(completedTask, delayTask);

                if (delayTask.IsCompleted)
                    throw new TimeoutException();

                if (!completedTask.Result.IsCompletedSuccessfully)
                    processRequestTasks.Remove(completedTask.Result);
                else
                    return ((Task<string>)completedTask.Result).Result;
            }
            throw new Exception();
        }

        protected override ILog Log => LogManager.GetLogger(typeof(RandomClusterClient));
    }
}
