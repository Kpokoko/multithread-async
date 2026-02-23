using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using log4net;

namespace ClusterClient.Clients
{
    public class SmartClusterClient : ClusterClientBase
    {
        private readonly ReplicasStatistics _replicasStatistics;

        public SmartClusterClient(string[] replicaAddresses) : base(replicaAddresses)
        {
            _replicasStatistics = new ReplicasStatistics(replicaAddresses);
        }

        public override async Task<string> ProcessRequestAsync(string query, TimeSpan timeout)
        {
            var timer = Stopwatch.StartNew();
            var sortedReplicas = ReplicaAddresses
                .OrderBy(x => _replicasStatistics.GetStats(x))
                .ToArray();
            var requests = new Dictionary<Task<string>, (string, DateTime)>();
            for (int i = 0; i < sortedReplicas.Length; i++)
            {
                var remainingTime = timeout - timer.Elapsed;
                var timeoutForReplica = TimeSpan.FromTicks(remainingTime.Ticks / (sortedReplicas.Length - i));
                var timeoutTask = Task.Delay(timeoutForReplica);
                var request = CreateRequest(sortedReplicas[i] + "?query=" + query);
                requests.Add(ProcessRequestAsync(request), (sortedReplicas[i], DateTime.Now));
                var requestResult = await Task.WhenAny(requests.Keys.Cast<Task>().Concat(new[] { timeoutTask }));
                if (requestResult != timeoutTask)
                {
                    var result = (Task<string>)requestResult;
                    var replicaData = requests[result];
                    var time = DateTime.Now - replicaData.Item2;
                    if (result.Status == TaskStatus.RanToCompletion)
                    {
                        _replicasStatistics.UpdateStats(replicaData.Item1, time.TotalMilliseconds);
                        requests.Remove(result);
                        return result.Result;
                    }
                    _replicasStatistics.UpdateStats(replicaData.Item1, time.TotalMilliseconds);
                    requests.Remove(result);
                }
            }

            throw new TimeoutException();
        }

        protected override ILog Log => LogManager.GetLogger(typeof(SmartClusterClient));
    }
}