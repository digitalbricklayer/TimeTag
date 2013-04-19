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
    using System.Collections.Generic;

    /// <summary>
    /// Template used to create a time series database
    /// </summary>
    public class TimeSeriesDatabaseTemplate
    {
        // Database title
        private string title;

        // Timestamp when the database is due to start
        private DateTime startTime;

        // Data source templates indexed by name
        private Dictionary<string, DataSourceTemplate> dataSources = new Dictionary<string, DataSourceTemplate>();

        // Archive templates indexed by name
        private Dictionary<string, ArchiveTemplate> archives = new Dictionary<string, ArchiveTemplate>();

        // Counter for the number of archive templates
        private int sequenceNumberCounter;

        /// <summary>
        /// Gets the database title
        /// </summary>
        public string Title
        {
            get { return this.title; }
            set { this.title = value; }
        }

        /// <summary>
        /// Gets the timestamp for the start of the database
        /// </summary>
        public DateTime StartTime
        {
            get { return this.startTime; }
            set { this.startTime = value; }
        }

        /// <summary>
        /// Gets the data source templates to be contained inside the database
        /// </summary>
        public IEnumerable<DataSourceTemplate> DataSources
        {
            get { return this.dataSources.Values; }
        }

        /// <summary>
        /// Gets the archive templates to be contained inside the database
        /// </summary>
        public IEnumerable<ArchiveTemplate> Archives
        {
            get { return this.archives.Values; }
        }

        /// <summary>
        /// Add a new data source template
        /// </summary>
        /// <param name="dataSourceName">Name of the data source</param>
        /// <param name="type">Type of the data source must be GAUGE</param>
        /// <param name="interval">Interval between readings measured in seconds</param>
        /// <param name="min">Minimum threshold for readings any reading below this threshold will be discarded</param>
        /// <param name="max">Maximum threshold for readings any reading above this threshold will be discarded</param>
        public void AddDataSource(string dataSourceName, DataSource.ConversionFunctionType type, TimeSpan interval, double min, double max)
        {
            AddDataSource(new DataSourceTemplate(dataSourceName, type, interval, min, max));
        }

        /// <summary>
        /// Add a new data source template
        /// </summary>
        /// <param name="newDataSourceTemplate">New data source template</param>
        public void AddDataSource(DataSourceTemplate newDataSourceTemplate)
        {
            // Validate the data source template info
            if (newDataSourceTemplate.Name.Length == 0 || newDataSourceTemplate.Name.Length > DataSource.MaxNameLength)
            {
                throw new ArgumentException(string.Format("Invalid data source name: {0}", newDataSourceTemplate.Name));
            }
            else if (newDataSourceTemplate.MinThreshold >= newDataSourceTemplate.MaxThreshold)
            {
                throw new ArgumentException(
                    string.Format("Max threshold {1} is lower than the lower threshold {0}", newDataSourceTemplate.MinThreshold, newDataSourceTemplate.MaxThreshold));
            }
            else if (newDataSourceTemplate.PollingInterval.TotalSeconds < 1)
            {
                throw new ArgumentException(string.Format("Invalid interval: {0}", newDataSourceTemplate.PollingInterval));
            }
            else if (dataSources.ContainsKey(newDataSourceTemplate.Name.ToLower()))
            {
                throw new ArgumentException(string.Format("Duplicate data source: {0}", newDataSourceTemplate.Name));
            }

            // Template info is ok, add it and index by its name
            this.dataSources[newDataSourceTemplate.Name.ToLower()] = newDataSourceTemplate;
        }

        /// <summary>
        /// Add an archive to the template. Each archive is added to all data sources.
        /// </summary>
        /// <param name="archiveName">Name of the archive</param>
        /// <param name="type">Type of the archive can be AVERAGE, MIN OR MAX</param>
        /// <param name="xFactor">The percentage of readings that can be missing before a datum is considered invalid</param>
        /// <param name="readingsPerDataPoint">Number of readings that make up a single data point</param>
        /// <param name="numDataPointPerArchive">Maximum number of data points contained in the archive</param>
        public void AddArchive(string archiveName, ArchiveTemplate.ConsolidationFunctionType type, int xFactor, int readingsPerDataPoint, int numDataPointPerArchive)
        {
            AddArchive(new ArchiveTemplate(archiveName, type, xFactor, readingsPerDataPoint, numDataPointPerArchive));
        }

        /// <summary>
        /// Add an archive template. Each archive is added to all data sources
        /// </summary>
        /// <param name="newArchiveTemplate">New archive template</param>
        public void AddArchive(ArchiveTemplate newArchiveTemplate)
        {
            // Validate the archive template info
            if (newArchiveTemplate.Name.Length == 0 || newArchiveTemplate.Name.Length > ArchiveTemplate.MaxNameLength)
            {
                throw new ArgumentException(string.Format("Invalid archive name: {0}", newArchiveTemplate.Name));
            }
            else if (newArchiveTemplate.ConsolidationFunction != ArchiveTemplate.ConsolidationFunctionType.Average &&
                     newArchiveTemplate.ConsolidationFunction != ArchiveTemplate.ConsolidationFunctionType.Min &&
                     newArchiveTemplate.ConsolidationFunction != ArchiveTemplate.ConsolidationFunctionType.Max &&
                     newArchiveTemplate.ConsolidationFunction != ArchiveTemplate.ConsolidationFunctionType.Last)
            {
                throw new ArgumentException(string.Format("Invalid archive type: {0}", newArchiveTemplate.ConsolidationFunction));
            }
            else if (newArchiveTemplate.XFactor < 1 || newArchiveTemplate.XFactor >= 100)
            {
                throw new ArgumentException(string.Format("Invalid XFactor: {0}", newArchiveTemplate.XFactor));
            }
            else if (archives.ContainsKey(newArchiveTemplate.Name.ToLower()))
            {
                throw new ArgumentException(string.Format("Duplicate archive name: {0}", newArchiveTemplate.Name));
            }

            newArchiveTemplate.SequenceNumber = this.sequenceNumberCounter++;

            // Template info is ok, add it and index by its name
            this.archives[newArchiveTemplate.Name.ToLower()] = newArchiveTemplate;
        }

        /// <summary>
        /// Gets the data source template corresponding to the given name
        /// </summary>
        /// <param name="nameToFind">Name of the data source to find</param>
        /// <returns>Data source template corresponding to the given name</returns>
        public DataSourceTemplate GetDataSourceByName(string nameToFind)
        {
            string nameToFindLowerCase = nameToFind.ToLower();
            return this.dataSources.ContainsKey(nameToFindLowerCase) ? this.dataSources[nameToFindLowerCase] : null;
        }

        /// <summary>
        /// Gets the archive template corresponding to the given name
        /// </summary>
        /// <param name="nameToFind">Name of the archive to find</param>
        /// <returns>Archive template corresponding to the given name</returns>
        public ArchiveTemplate GetArchiveByName(string nameToFind)
        {
            string nameToFindLowerCase = nameToFind.ToLower();
            return this.archives.ContainsKey(nameToFindLowerCase) ? this.archives[nameToFindLowerCase] : null;
        }
    }
}
