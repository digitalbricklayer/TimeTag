namespace Openxtra.TimeTag.Database
{
    using System;
    using System.Diagnostics;

    internal class DeriveConversionFunction : IConversionFunction
    {
        private DataSource dataSource;

        public DeriveConversionFunction(DataSource dataSource)
        {
            this.dataSource = dataSource;
        }

        public Reading PreProcessReading(Reading newReading)
        {
            Debug.Assert(newReading.Timestamp > this.dataSource.LastUpdateTimestamp);

            if (this.dataSource.LastReading != null)
            {
                TimeSpan timeSpanFromLastReading = newReading.Timestamp - this.dataSource.LastUpdateTimestamp;
                double processedValue = (newReading.Value - this.dataSource.LastReading.Value) / Convert.ToInt64(timeSpanFromLastReading.TotalSeconds);
                return new Reading(processedValue, newReading.Timestamp);
            }
            else
            {
                return new Reading(Double.NaN, newReading.Timestamp);
            }
        }
    }
}
