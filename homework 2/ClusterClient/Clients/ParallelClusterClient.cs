using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using log4net;

namespace ClusterClient.Clients
{
    public class ParallelClusterClient : ClusterClientBase
    {
        public ParallelClusterClient(string[] replicaAddresses) : base(replicaAddresses)
        {
        }

        public override async Task<string> ProcessRequestAsync(string query, TimeSpan timeout)
        {
            var tasks = new List<Task<string>>();
            for (var i = 0; i < ReplicaAddresses.Length; ++i)
            {
                var request = CreateRequest(ReplicaAddresses[i] + "?query=" + query);
                tasks.Add(ProcessRequestAsync(request));
            }

            var timeoutTask = Task.Delay(timeout).ContinueWith<string>(_ => throw new TimeoutException());

            while (tasks.Count > 0)
            {
                var completedTask = await Task.WhenAny(tasks.Cast<Task>().Concat(new[] {timeoutTask}));
                if (completedTask == timeoutTask)
                    throw new TimeoutException();
                try
                {
                    return await (Task<string>)completedTask;
                }
                catch
                {
                    tasks.Remove((Task<string>)completedTask);
                }
            }
            throw new TimeoutException();
        }

        protected override ILog Log => LogManager.GetLogger(typeof(ParallelClusterClient));
    }
}
