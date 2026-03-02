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
                if (remainingTime <= TimeSpan.Zero)
                    throw new TimeoutException();
                var timeoutForReplica = TimeSpan.FromTicks(remainingTime.Ticks / (sortedReplicas.Length - i));
                if (timeoutForReplica <= TimeSpan.Zero)
                    throw new TimeoutException();
                var timeoutTask = Task.Delay(timeoutForReplica);
                var request = CreateRequest(sortedReplicas[i] + "?query=" + query);
                requests.Add(ProcessRequestAsync(request), (sortedReplicas[i], DateTime.Now));
                var requestResult = await Task.WhenAny(requests.Keys.Concat(new[] { timeoutTask }));
                if (requestResult != timeoutTask)
                {
                    var result = (Task<string>)requestResult;
                    var replicaData = requests[result];
                    var time = DateTime.Now - replicaData.Item2;
                    if (result.Status == TaskStatus.RanToCompletion)
                    {
                        _replicasStatistics.UpdateStats(replicaData.Item1, time.TotalMilliseconds);
                        requests.Remove(result);
                        foreach (var badRequest in requests)
                            _replicasStatistics.UpdateStats(badRequest.Value.Item1, timeoutForReplica.TotalMilliseconds);
                        requests.Clear();
                        return result.Result;
                    }
                    _replicasStatistics.UpdateStats(replicaData.Item1, time.TotalMilliseconds);
                    requests.Remove(result);
                }
                else
                    _replicasStatistics.UpdateStats(sortedReplicas[i], timeoutForReplica.TotalMilliseconds);
            }

            throw new TimeoutException();
        }

        protected override ILog Log => LogManager.GetLogger(typeof(SmartClusterClient));
    }
}