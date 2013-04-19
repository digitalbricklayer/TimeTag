namespace Openxtra.TimeTag.Database
{
    using System;

    internal class GaugeConversionFunction : IConversionFunction
    {
        private DataSource dataSource;

        public GaugeConversionFunction(DataSource dataSource)
        {
            this.dataSource = dataSource;
        }

        /// <summary>
        /// Pre-process a reading. The gauge doesn't do any pre-processing of the
        /// reading.
        /// </summary>
        /// <param name="newReading">Reading to process</param>
        /// <returns>Processed reading</returns>
        public Reading PreProcessReading(Reading newReading)
        {
            // Gauge's don't need to pre-process the raw reading
            return newReading;
        }
    }
}
