using System.Collections.Concurrent;
using System.Linq;

namespace ClusterClient;

public class ReplicasStatistics
{
    private const double ConfidenceFactor = 0.8;
    private readonly ConcurrentDictionary<string, double> _replicaStatistics;

    public ReplicasStatistics(string[] replicaAddresses)
    {
        _replicaStatistics = new ConcurrentDictionary<string, double>();
        foreach (var replicaAddress in replicaAddresses)
            _replicaStatistics.TryAdd(replicaAddress, 0);
    }

    public void UpdateStats(string replicaAddress, double workTime)
    {
        _replicaStatistics[replicaAddress] =
            _replicaStatistics[replicaAddress] * ConfidenceFactor + workTime * (1 - ConfidenceFactor);
    }

    public double GetStats(string replicaAddress)
    {
        return _replicaStatistics.TryGetValue(replicaAddress, out double value) ? value : 0;
    }
}