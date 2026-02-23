using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using log4net;

namespace ClusterClient.Clients
{
    public class RoundRobinClusterClient : ClusterClientBase
    {
        private readonly ReplicasStatistics _replicasStatistics;
        public RoundRobinClusterClient(string[] replicaAddresses) : base(replicaAddresses)
        {
            _replicasStatistics = new ReplicasStatistics(replicaAddresses);
        }

        public override async Task<string> ProcessRequestAsync(string query, TimeSpan timeout)
        {
            var timer = Stopwatch.StartNew();
            var sortedReplicas = ReplicaAddresses
                .OrderBy(x => _replicasStatistics.GetStats(x))
                .ToArray();
            var sentTasks = new Dictionary<Task<string>, (string, DateTime)>();
            for (int i = 0; i < sortedReplicas.Length; i++)
            {
                var remainingTime = timeout - timer.Elapsed;
                var timeoutForReplica = TimeSpan.FromTicks(remainingTime.Ticks / (sortedReplicas.Length - i));
                var timeoutTask = Task.Delay(timeoutForReplica);
                var request = CreateRequest(sortedReplicas[i] + "?query=" + query);
                var task = ProcessRequestAsync(request);
                sentTasks.Add(task, (address: sortedReplicas[i], startTime: DateTime.Now));
                var requestResult = await Task.WhenAny(task, timeoutTask);
                if (requestResult != timeoutTask)
                {
                    var replicaData = sentTasks[task];
                    var time = DateTime.Now - replicaData.Item2;
                    var result = (Task<string>)requestResult;
                    if (result.Status == TaskStatus.RanToCompletion)
                    {
                        _replicasStatistics.UpdateStats(replicaData.Item1, time.TotalMilliseconds);
                        sentTasks.Remove(task);
                        return result.Result;
                    }
                    _replicasStatistics.UpdateStats(replicaData.Item1, timeout.TotalMilliseconds);
                    sentTasks.Remove(task);
                }
            }
            throw new TimeoutException();
        }

        protected override ILog Log => LogManager.GetLogger(typeof(RoundRobinClusterClient));
    }
}
