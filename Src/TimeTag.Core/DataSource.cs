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
    using System.Collections;
    using System.Collections.Generic;
    using System.Diagnostics;

    public sealed class DataSource
    {
        /// <summary>
        /// Maximum length of the data source name
        /// </summary>
        public const int MaxNameLength = 56;

        /// <summary>
        /// Maximum length of the type
        /// </summary>
        public const int MaxTypeLength = 16;

        /// <summary>
        /// Conversion functions that can modify the readings processed by the 
        /// data source. Only gauge is supported at the moment.
        /// </summary>
        public enum ConversionFunctionType { Gauge, Absolute, Derive, Counter }

        /// <summary>
        /// The unique name of the data source. Must not exceed MaxNameLength in size
        /// </summary>
        private string name;

        /// <summary>
        /// Conversion function that can pre-process a reading before it is 
        /// stored in an archive
        /// </summary>
        private ConversionFunctionType conversionFunction;

        // Cached conversion function object
        private IConversionFunction conversionFunctionObject;

        /// <summary>
        /// Range all valid values must be within. Readings that fall outside the set
        /// range will be rejected
        /// </summary>
        private Range range;

        /// <summary>
        /// Expected interval between successive readings. Readings outside 
        /// of the maximum will be ignored.
        /// </summary>
        private TimeSpan pollingInterval;

        /// <summary>
        /// Collection of archives contained in the data source
        /// </summary>
        private Archive.ArchiveCollection archives;

        /// <summary>
        /// Data access object (DAO) for read/writing to the persistence mechanism
        /// </summary>
        private IDataSourceDao dao;

        /// <summary>
        /// Database in which the data source is embedded
        /// </summary>
        private TimeSeriesDatabase database;

        /// <summary>
        /// Last reading received by the data source
        /// </summary>
        private Reading lastReading;

        // The data source statistics
        private DataSourceStats stats;

        /// <summary>
        /// Initialises a data source
        /// </summary>
        /// <param name="database">Database to exist within</param>
        /// <param name="template">Data source template</param>
        internal DataSource(TimeSeriesDatabase database, DataSourceTemplate template)
        {
            this.database = database;
            this.Name = template.Name;
            this.ConversionFunction = template.ConversionFunction;
            this.CreateConversionFunction();
            this.PollingInterval = template.PollingInterval;
            this.range = new Range(template.MinThreshold, template.MaxThreshold);
            this.archives = new Archive.ArchiveCollection(this);
            this.stats = new DataSourceStats(this);
        }

        internal DataSource(TimeSeriesDatabase database, DataSourceDto dto)
        {
            this.database = database;
            this.archives = new Archive.ArchiveCollection(this);
            this.stats = new DataSourceStats(this);
            this.range = new Range();
            FixupProperties(dto);
            FixupArchives(dto);
        }

        /// <summary>
        /// Gets the data source name
        /// </summary>
        public string Name
        {
            get { return this.name; }
            internal set { this.name = value; }
        }

        /// <summary>
        /// Gets the data source conversion function
        /// </summary>
        public ConversionFunctionType ConversionFunction
        {
            get { return this.conversionFunction; }
            internal set { this.conversionFunction = value; }
        }

        /// <summary>
        /// Gets the range defining which are valid values and which 
        /// will be discarded
        /// </summary>
        public Range Range
        {
            get { return this.range; }
        }

        /// <summary>
        /// Gets the data source polling interval
        /// </summary>
        public TimeSpan PollingInterval
        {
            get { return this.pollingInterval; }
            private set { this.pollingInterval = value; }
        }

        /// <summary>
        /// Gets a collection of archives
        /// </summary>
        public Archive.ArchiveCollection Archives
        {
            get { return this.archives; }
        }

        /// <summary>
        /// Gets the database start time
        /// </summary>
        public DateTime StartTime
        {
            get { return this.database.StartTime; }
        }

        /// <summary>
        /// Gets the data source last update time
        /// </summary>
        public DateTime LastUpdateTimestamp
        {
            get
            {
                if (this.lastReading != null)
                {
                    return this.lastReading.Timestamp;
                }
                else
                {
                    return DateTime.MinValue;
                }
            }
        }

        /// <summary>
        /// Gets the reading statistics
        /// </summary>
        public DataSourceStats Stats
        {
            get { return this.stats; }
        }

        /// <summary>
        /// Gets the last reading
        /// </summary>
        public Reading LastReading
        {
            get { return this.lastReading; }
        }

        internal IDataSourceDao Dao
        {
            get { return this.dao; }
            set { this.dao = value; }
        }

        /// <summary>
        /// Push a collection of readings into each archive for consolidation
        /// </summary>
        /// <param name="newReadings">Readings to be consolidated</param>
        public void Push(Reading[] newReadings)
        {
            List<Reading> validReadings = new List<Reading>();

            DataSourceStats originalStats = this.stats.Clone() as DataSourceStats;

            /*
             * Validate the new readings are within the thresholds specified 
             * for the data source and are not timestamped as being recorded
             * prior to the most recent reading
             */
            foreach (Reading newReading in newReadings)
            {
                // Make sure the value is within the expected range
                if (this.range.IsValid(newReading.Value) != true)
                {
                    // ERROR: Reading is outside expected range
                    this.stats.IncrementDiscarded();
                    continue;
                }
                
                if (newReading.Timestamp <= this.LastUpdateTimestamp)
                {
                    /*
                     * ERROR: Reading timestamp is prior to the most recent 
                     * timestamp seen by the data source
                     */
                    this.stats.IncrementDiscarded();
                    continue;
                }

                Reading processedReading = this.CreateConversionFunction().PreProcessReading(newReading);

                validReadings.Add(processedReading);
                /*
                 * The last reading should be the none-processed reading, not the one
                 * that has been processed by the conversion function
                 */
                this.lastReading = newReading;
                this.stats.IncrementTotal();
            }

            if (validReadings.Count > 0)
            {
                /*
                 * Push the new readings into each archive in turn...this will result 
                 * in zero or more data points being consolidated inside each archive
                 */
                foreach (Archive archive in this.archives)
                {
                    archive.Push(validReadings.ToArray());
                }

                // Save the last reading
                PersistLastReading();
            }

            if (originalStats != this.stats)
            {
                // Save the new reading stats
                this.stats.Update();
            }
        }

        /// <summary>
        /// Gets an archive given the name of the archive
        /// </summary>
        /// <param name="nameToFind"></param>
        /// <returns></returns>
        public Archive GetArchiveByName(string nameToFind)
        {
            return this.archives.GetArchiveByName(nameToFind);
        }

        /// <summary>
        /// Sets the data source name
        /// </summary>
        /// <param name="newTitle">New data source name</param>
        public void SetName(string newName)
        {
            Debug.Assert(this.dao != null);

            if (newName == this.Name)
            {
                // The name hasn't changed...
                return;
            }

            if (newName.Length > DataSource.MaxNameLength)
            {
                // ERROR: the new name is too long
                throw new ArgumentException("Data source name is too long");
            }

            string oldName = this.Name;

            this.Name = newName;

            // Persist the new name
            this.dao.SetName(newName);

            // The data sources are index by name in the database
            this.database.RenameDataSource(oldName, newName);
        }

        internal void Create()
        {
            this.dao = this.database.GetDaoFactory().CreateDataSource(this.database.Dao);
            this.dao.Create(CreateDto());
            this.stats.Create();
            this.archives.Create();
        }

        internal void AddArchives(ArchiveTemplate.ArchiveTemplateCollection templates)
        {
            foreach (ArchiveTemplate archiveTemplate in templates)
            {
                this.archives.Add(CreateArchive(archiveTemplate));
            }
        }

        internal DataSourceDto CreateDto()
        {
            DataSourceDto dto = new DataSourceDto();
            dto.Dao = this.dao;
            dto.Name = this.Name;
            dto.ConversionFunction = (int) this.conversionFunction;
            dto.Range = this.range.CreateDto();
            dto.PollingInterval = this.PollingInterval;
            if (this.lastReading != null)
            {
                dto.LastReading = this.lastReading.CreateDto();
            }
            dto.Stats = this.stats.CreateDto();
            foreach (Archive ar in this.archives)
            {
                dto.AddArchive(ar.CreateDto());
            }
            return dto;
        }

        internal IDaoFactory GetDaoFactory()
        {
            // Delegate to the database, it knows which DAOs are being used
            return this.database.GetDaoFactory();
        }

        private void FixupArchives(DataSourceDto dto)
        {
            foreach (ArchiveDto archiveDto in dto.Archives)
            {
                Archive newArchive = CreateArchive(archiveDto);
                newArchive.FixupFromDto(archiveDto);
                this.archives.Add(newArchive);
            }
        }

        private void FixupProperties(DataSourceDto dto)
        {
            this.Dao = dto.Dao;
            this.Name = dto.Name;
            this.ConversionFunction = (ConversionFunctionType) dto.ConversionFunction;
            this.CreateConversionFunction();
            this.PollingInterval = dto.PollingInterval;
            if (dto.LastReading != null)
            {
                if (dto.LastReading.Empty != true)
                {
                    this.lastReading = new Reading();
                    this.lastReading.FixupFromDto(dto.LastReading);
                }
            }
            this.range.FixupFromDto(dto.Range);
            this.Stats.FixupFromDto(dto.Stats);
        }

        private Archive CreateArchive(ArchiveTemplate template)
        {
            return new Archive(this, template);
        }

        private Archive CreateArchive(ArchiveDto dto)
        {
            return CreateArchive(this.database.GetArchiveTemplateByIndex(dto.SequenceNumber));
        }

        private void PersistLastReading()
        {
            Debug.Assert(this.dao != null);

            this.dao.UpdateLastReading(this.lastReading.CreateDto());
        }

        private void SaveSlotExpiryTime()
        {
            foreach (Archive archive in this.archives)
            {
                archive.SaveSlotExpiryTime();
            }
        }

        private IConversionFunction CreateConversionFunction()
        {
            if (this.conversionFunctionObject != null)
            {
                return this.conversionFunctionObject;
            }
            else
            {
                IConversionFunction function;

                switch (this.conversionFunction)
                {
                    case ConversionFunctionType.Absolute:
                        function = new AbsoluteConversionFunction(this);
                        break;

                    case ConversionFunctionType.Counter:
                        function = new CounterConversionFunction(this);
                        break;

                    case ConversionFunctionType.Derive:
                        function = new DeriveConversionFunction(this);
                        break;

                    case ConversionFunctionType.Gauge:
                        function = new GaugeConversionFunction(this);
                        break;

                    default:
                        throw new ArgumentException("Invalid conversion function");
                }

                this.conversionFunctionObject = function;

                return this.conversionFunctionObject;
            }
        }

        /// <summary>
        /// Collection of data sources
        /// </summary>
        public class DataSourceCollection : IEnumerable
        {
            // Data sources indexed by name
            private Dictionary<string, DataSource> dataSources = new Dictionary<string, DataSource>();

            /// <summary>
            /// Gets the number of data sources in the collection
            /// </summary>
            public int Count
            {
                get { return this.dataSources.Count; }
            }

            internal DataSourceCollection()
            {
            }

            /// <summary>
            /// Add a new data source to the collection
            /// </summary>
            /// <param name="newDataSource"></param>
            public void Add(DataSource newDataSource)
            {
                this.dataSources.Add(newDataSource.Name.ToLower(), newDataSource);
            }

            /// <summary>
            /// Clear the contents of the data source collection
            /// </summary>
            public void Clear()
            {
                this.dataSources.Clear();
            }

            /// <summary>
            /// Return an enumerator for the collection
            /// </summary>
            /// <returns>Enumerator object</returns>
            public IEnumerator GetEnumerator()
            {
                return this.dataSources.Values.GetEnumerator();
            }

            /// <summary>
            /// Return the data source referenced by the given name
            /// </summary>
            /// <param name="dataSourceName">Name of the data source to find</param>
            /// <returns>Data source found of null if no data source can be found with 
            /// the given name</returns>
            public DataSource GetDataSourceByName(string dataSourceName)
            {
                string dataSourceNameLowerCase = dataSourceName.ToLower();
                if (this.dataSources.ContainsKey(dataSourceNameLowerCase))
                {
                    return this.dataSources[dataSourceNameLowerCase];
                }
                else
                {
                    // The data source specified doesn't exist
                    return null;
                }
            }

            /// <summary>
            /// Returns true if the data source with the given name exists
            /// </summary>
            /// <param name="dataSourceName">Name of the data source</param>
            /// <returns>True if the data source is found, false otherwise</returns>
            public bool Exists(string dataSourceName)
            {
                string dataSourceNameLowerCase = dataSourceName.ToLower();
                return this.dataSources.ContainsKey(dataSourceNameLowerCase);
            }

            internal void RenameDataSource(string oldName, string newName)
            {
                DataSource renamedDataSource = this.dataSources[oldName.ToLower()];
                this.dataSources.Remove(oldName.ToLower());
                this.dataSources.Add(newName.ToLower(), renamedDataSource);
            }
        }
    }
}
