using System;
using System.Collections.Generic;
using System.Drawing;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace ClusterClient.Clients
{
    public class TimeStat
    {
        private const int size = 50;

        private readonly TimeSpan[] times = new TimeSpan[size];
        private int i;

        public void Add(TimeSpan time)
        {
            times[i % size] = time;
            i++;
        }

        public double AverageMilliseconds()
        {
            return times.Select(ts => ts.Milliseconds).Average();
        }
    }
}
