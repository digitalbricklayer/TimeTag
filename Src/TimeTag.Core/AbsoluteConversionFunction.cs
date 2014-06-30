namespace Openxtra.TimeTag.Database
{
    using System;
    using System.Diagnostics;

    internal class AbsoluteConversionFunction : IConversionFunction
    {
        DataSource dataSource;

        public AbsoluteConversionFunction(DataSource dataSource)
        {
            this.dataSource = dataSource;
        }

        public Reading PreProcessReading(Reading newReading)
        {
            Debug.Assert(newReading.Timestamp > this.dataSource.LastUpdateTimestamp);

            DateTime lastUpdateTimestamp;

            if (this.dataSource.LastUpdateTimestamp == DateTime.MinValue)
            {
                lastUpdateTimestamp = this.dataSource.StartTime;
            }
            else
            {
                lastUpdateTimestamp = this.dataSource.LastUpdateTimestamp;
            }

            TimeSpan timeSpanFromLastReading = newReading.Timestamp - lastUpdateTimestamp;
            double processedValue = newReading.Value / Convert.ToInt64(timeSpanFromLastReading.TotalSeconds);

            // Retain the existing timestamp
            return new Reading(processedValue, newReading.Timestamp);
        }
    }
}
