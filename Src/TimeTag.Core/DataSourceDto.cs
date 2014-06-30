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
    /// Data Transfer Object for the data source
    /// </summary>
    internal class DataSourceDto
    {
        private IDataSourceDao dao;
        private string name;
        private int conversionFunction;
        private TimeSpan pollingInterval;
        private RangeDto range;
        private ReadingDto lastReading;
        private DataSourceStatsDto stats;

        // Archive DTOs
        private ArchiveCollectionDto archives = new ArchiveCollectionDto();

        /// <summary>
        /// Gets the data access object for this DTO used for reading and writing 
        /// to the storage medium
        /// </summary>
        public IDataSourceDao Dao
        {
            get { return this.dao; }
            set { this.dao = value; }
        }

        /// <summary>
        /// Gets the name of the archive
        /// </summary>
        public string Name
        {
            get { return this.name; }
            set { this.name = value; }
        }

        /// <summary>
        /// Gets the type of archive must be one of: GAUGE
        /// </summary>
        public int ConversionFunction
        {
            get { return this.conversionFunction; }
            set { this.conversionFunction = value; }
        }

        /// <summary>
        /// Gets the interval between readings in seconds
        /// </summary>
        public TimeSpan PollingInterval
        {
            get { return this.pollingInterval; }
            set { this.pollingInterval = value; }
        }

        /// <summary>
        /// Gets the minimum value permitted inside the data source
        /// </summary>
        public RangeDto Range
        {
            get { return this.range; }
            set { this.range = value; }
        }

        /// <summary>
        /// Gets the last reading processed by the data source
        /// </summary>
        public ReadingDto LastReading
        {
            get { return this.lastReading; }
            set { this.lastReading = value; }
        }

        /// <summary>
        /// Gets the collection of archive DTO contained in the data source
        /// </summary>
        public ArchiveCollectionDto Archives
        {
            get { return this.archives; }
            set { this.archives = value; }
        }

        /// <summary>
        /// Gets the reading stats DTO
        /// </summary>
        public DataSourceStatsDto Stats
        {
            get { return this.stats; }
            set { this.stats = value; }
        }

        /// <summary>
        /// Add a new archive DTO
        /// </summary>
        /// <param name="newArchiveDto"></param>
        public void AddArchive(ArchiveDto newArchiveDto)
        {
            this.archives.Add(newArchiveDto);
        }
    }
}
