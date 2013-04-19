namespace Openxtra.TimeTag.Database
{
    using System;
    using System.Diagnostics;

    internal class CounterConversionFunction : IConversionFunction
    {
        private DataSource dataSource;

        public CounterConversionFunction(DataSource dataSource)
        {
            this.dataSource = dataSource;
        }

        public Reading PreProcessReading(Reading newReading)
        {
            Debug.Assert(newReading.Timestamp > this.dataSource.LastUpdateTimestamp);

            if (this.dataSource.LastReading != null)
            {
                double processedValue = Double.NaN;
                double diff = newReading.Value - this.dataSource.LastReading.Value;
                if (diff < 0)
                {
                    diff += Int32.MaxValue;
                }
                if (diff < 0)
                {
                    diff += Int64.MaxValue - Int32.MaxValue;
                }
                if (diff >= 0)
                {
                    TimeSpan timeSpanFromLastReading = newReading.Timestamp - this.dataSource.LastUpdateTimestamp;
                    processedValue = diff / Convert.ToInt64(timeSpanFromLastReading.TotalSeconds);
                }
                return new Reading(processedValue, newReading.Timestamp);
            }
            else
            {
                return new Reading(Double.NaN, newReading.Timestamp);
            }
        }
    }
}
