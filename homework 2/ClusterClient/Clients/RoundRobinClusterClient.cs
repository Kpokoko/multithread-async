using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using log4net;

namespace ClusterClient.Clients
{
    public class RoundRobinClusterClient : ClusterClientBase
    {
        public RoundRobinClusterClient(string[] replicaAddresses) : base(replicaAddresses)
        {
        }

        public override async Task<string> ProcessRequestAsync(string query, TimeSpan timeout)
        {
            var timeoutForReplica = timeout / ReplicaAddresses.Length;
            foreach (var replicaAddress in ReplicaAddresses)
            {
                var timeoutTask = Task.Delay(timeoutForReplica);
                var request = CreateRequest(replicaAddress + "?query=" + query);
                var requestResult = await Task.WhenAny(ProcessRequestAsync(request), timeoutTask);
                if (requestResult != timeoutTask)
                {
                    var result = (Task<string>)requestResult;
                    if (result.Status == TaskStatus.RanToCompletion)
                        return result.Result;
                }
            }
            throw new TimeoutException();
        }

        protected override ILog Log => LogManager.GetLogger(typeof(RoundRobinClusterClient));
    }
}
