using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using log4net;

namespace ClusterClient.Clients
{
    public class SmartClusterClient : ClusterClientBase
    {
        public SmartClusterClient(string[] replicaAddresses) : base(replicaAddresses)
        {
        }

        public override async Task<string> ProcessRequestAsync(string query, TimeSpan timeout)
        {
            var timeoutForReplica = timeout / ReplicaAddresses.Length;
            var requests = new List<Task<string>>();
            foreach (var replicaAddress in ReplicaAddresses)
            {
                var timeoutTask = Task.Delay(timeoutForReplica);
                var request = CreateRequest(replicaAddress + "?query=" + query);
                requests.Add(ProcessRequestAsync(request));
                var requestResult = await Task.WhenAny(requests.Cast<Task>().Concat(new[] {timeoutTask}));
                if (requestResult != timeoutTask)
                {
                    var result = (Task<string>)requestResult;
                    if (result.Status == TaskStatus.RanToCompletion)
                        return result.Result;
                    requests.Remove(result);
                }
            }
            throw new TimeoutException();
        }

        protected override ILog Log => LogManager.GetLogger(typeof(SmartClusterClient));
    }
}
