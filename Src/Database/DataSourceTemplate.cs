/*
 * Copyright 2008 OPENXTRA Limited
 * 
 * This file is part of TimeTag.
 * 
 * TimeTag is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 * 
 * TimeTag is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 * 
 * You should have received a copy of the GNU General Public License
 * along with TimeTag.  If not, see <http://www.gnu.org/licenses/>.
 */

namespace Openxtra.TimeTag.Database
{
    using System;

    public class DataSourceTemplate
    {
        private string name;
        private DataSource.ConversionFunctionType conversionFunction;
        private TimeSpan pollingInterval;
        private double minThreshold;
        private double maxThreshold;

        /// <summary>
        /// Initialises a data source template
        /// </summary>
        /// <param name="dataSourceName">Name of the data source</param>
        /// <param name="type">Type of the data source. Valid values are: GAUGE</param>
        /// <param name="intervalInSeconds">Interval in seconds between readings</param>
        /// <param name="minThreshold">Minimum value that is acceptable for a reading to be considered valid</param>
        /// <param name="maxThreshold">Maximum value that is acceptable for a reading to be considered valid</param>
        public DataSourceTemplate(string dataSourceName, DataSource.ConversionFunctionType conversionFunction, TimeSpan pollingInterval, double min, double max)
        {
            this.Name = dataSourceName;
            this.ConversionFunction = conversionFunction;
            this.PollingInterval = pollingInterval;
            this.MinThreshold = min;
            this.MaxThreshold = max;
        }

        /// <summary>
        /// Name of the data source
        /// </summary>
        public string Name
        {
            get { return this.name; }
            set { this.name = value; }
        }

        /// <summary>
        /// Type of the data source can be one of: GAUGE
        /// </summary>
        public DataSource.ConversionFunctionType ConversionFunction
        {
            get { return this.conversionFunction; }
            set { this.conversionFunction = value; }
        }

        /// <summary>
        /// Interval in seconds between polls
        /// </summary>
        public TimeSpan PollingInterval
        {
            get { return this.pollingInterval; }
            set { this.pollingInterval = value; }
        }

        /// <summary>
        /// Minimum value that is acceptable for a reading to be considered 
        /// valid. Readings below min are discarded
        /// </summary>
        public double MinThreshold
        {
            get { return this.minThreshold; }
            set { this.minThreshold = value; }
        }

        /// <summary>
        /// Maximum value that is acceptable for a reading to be considered 
        /// valid. Readings above max are discarded
        /// </summary>
        public double MaxThreshold
        {
            get { return this.maxThreshold; }
            set { this.maxThreshold = value; }
        }
    }
}
