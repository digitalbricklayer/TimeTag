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

    /// <summary>
    /// An archive consists of a set of consolidated data points created from 
    /// one or more readings
    /// </summary>
    public sealed class Archive
    {
        // The template of the archive
        private ArchiveTemplate template;

        /// <summary>
        /// Timestamp when the current slot expires
        /// </summary>
        private DateTime slotExpiryTimestamp;

        /// <summary>
        /// Data point circular queue
        /// </summary>
        private DataPoint.DataPointCircularCollection dataPointCircularQueue;

        /// <summary>
        /// Accumulated readings for the current slot
        /// </summary>
        private Reading.ReadingCollection accumulatedReadings;

        /// <summary>
        /// DAO for persisting the archive to the database
        /// </summary>
        private IArchiveDao dao;

        /// <summary>
        /// Data source into which this archive is embedded
        /// </summary>
        private DataSource dataSource;

        // Strategy for writing to the database
        private enum PersistenceStrategy { DelayWrite, ImmediateWrite }

        // Cached consolidation function
        private IConsolidationFunction consolidationFunction;

        /// <summary>
        /// Initialises an archive attached to the given data source
        /// </summary>
        /// <param name="dataSource">Data source to which the archive belongs</param>
        /// <param name="template">Template the archive</param>
        internal Archive(DataSource dataSource, ArchiveTemplate template)
        {
            this.dataSource = dataSource;
            this.template = template;
            this.dataPointCircularQueue = new DataPoint.DataPointCircularCollection(this, template.MaxDataPoints);
            this.accumulatedReadings = new Reading.ReadingCollection(this, template.ReadingsPerDataPoint);
            this.slotExpiryTimestamp = CalcNextSlotExpiryTime(this.dataSource.StartTime);
        }

        /// <summary>
        /// Gets the archive template
        /// </summary>
        public ArchiveTemplate Template
        {
            get { return this.template; }
        }

        /// <summary>
        /// Gets the name of the archive
        /// </summary>
        public string Name
        {
            get { return this.template.Name; }
        }

        /// <summary>
        /// Gets the consolidation function type can be one of: AVERAGE, MAX, MIN, LAST
        /// </summary>
        public ArchiveTemplate.ConsolidationFunctionType ConsolidationFunction
        {
            get { return this.template.ConsolidationFunction; }
        }

        /// <summary>
        /// Gets the percentage of readings that can be absent before a data point is considered invalid
        /// </summary>
        public int XFactor
        {
            get { return this.template.XFactor; }
        }

        /// <summary>
        /// Gets the number of readings that are processed to make a single data point
        /// </summary>
        public int NumReadingsPerDataPoint
        {
            get { return this.template.ReadingsPerDataPoint; }
        }

        /// <summary>
        /// Gets the maximum number of data points that can be stored in the archive
        /// </summary>
        public int MaxDataPoints
        {
            get { return this.template.MaxDataPoints; }
        }

        /// <summary>
        /// Gets the data points contained in the archive
        /// </summary>
        public DataPoint.DataPointCircularCollection DataPoints
        {
            get { return this.dataPointCircularQueue; }
        }

        /// <summary>
        /// Gets the timestamp when the current slot expires
        /// </summary>
        private DateTime SlotExpiryTimestamp
        {
            get { return this.slotExpiryTimestamp; }
        }

        /// <summary>
        /// Gets the accumulated readings not yet turned into a data point
        /// </summary>
        public Reading.ReadingCollection AccumulatedReadings
        {
            get { return this.accumulatedReadings; }
        }

        internal IArchiveDao Dao
        {
            get { return this.dao; }
        }

        /// <summary>
        /// Push a collection of readings into the archive for consolidation
        /// </summary>
        /// <param name="newReadings">Readings to consolidate</param>
        public void Push(Reading[] newReadings)
        {
            /*
             * Process each reading in turn but only persist the changes to 
             * disk at the end
             */
            foreach (Reading newReading in newReadings)
            {
                this.accumulatedReadings.Add(newReading);

                if (newReading.Timestamp >= this.SlotExpiryTimestamp)
                {
                    /* 
                     * The current slot has expired, add one or more NaN data points to the 
                     * queue as appropriate, a data point may be written if sufficient 
                     * readings have been received in the current slot
                     */
                    CreateDataPoint(newReading, PersistenceStrategy.DelayWrite);
                }
            }

            if (newReadings.Length > 0)
            {
                /*
                 * Persist changes to the archive to disk in one batch. Writing 
                 * to the database is expensive
                 */
                SaveChangesToDisk();
            }
        }

        /// <summary>
        /// Create the archive in the database
        /// </summary>
        internal void Create()
        {
            Debug.Assert(this.dao == null);

            this.dao = this.dataSource.GetDaoFactory().CreateArchive(this.dataSource.Dao);
            this.dao.Create(CreateDto());
            this.accumulatedReadings.Create();
            this.dataPointCircularQueue.Create();
        }

        internal void FixupFromDto(ArchiveDto dto)
        {
            this.dao = dto.Dao;
            this.slotExpiryTimestamp = dto.SlotExpiryTime;
            this.dataPointCircularQueue.FixupFromDto(dto.DataPointQueue);
            this.accumulatedReadings.FixupFromDto(dto.AccumulatedReadings);
        }

        internal ArchiveDto CreateDto()
        {
            ArchiveDto dto = new ArchiveDto(this.Template.SequenceNumber);
            dto.Dao = dao;
            dto.SlotExpiryTime = slotExpiryTimestamp;
            dto.DataPointQueue = this.dataPointCircularQueue.CreateDto();
            dto.AccumulatedReadings = this.accumulatedReadings.CreateDto();
            return dto;
        }

        /// <summary>
        /// Reset the accumulated readings
        /// </summary>
        private void ResetAccumulatedReadings()
        {
            this.accumulatedReadings.Reset();
        }

        /// <summary>
        /// Gets the number of readings accumulated
        /// </summary>
        internal int NumReadings
        {
            get { return this.accumulatedReadings.Count; }
        }

        internal void SaveSlotExpiryTime()
        {
            Debug.Assert(this.dao != null);

            // Save the new expiry time to the database
            this.dao.SaveExpiryTime(this.slotExpiryTimestamp);
        }

        private int CalcAccumulatedReadingsRatioAsPercentage()
        {
            Debug.Assert(this.template.ReadingsPerDataPoint > 0);
            Debug.Assert(this.NumReadings > 0);

            return (this.NumReadings / this.template.ReadingsPerDataPoint) * 100;
        }

        private void PushMissingDataPoints(DateTime currentReadingTime)
        {
            DateTime nextTime = this.slotExpiryTimestamp;
            while (nextTime < currentReadingTime)
            {
                this.dataPointCircularQueue.Add(DataPoint.CreateNaN(nextTime));
                nextTime = CalcNextSlotExpiryTime();
            }
        }

        private void SaveChangesToDisk()
        {
            // Save the slot expiry time
            SaveSlotExpiryTime();

            // Save the un-persisted changes to disk
            this.dataPointCircularQueue.PersistChanges();

            /*
             * Write new readings to the database...there isn't 
             * guaranteed to be a new one with selective archives
             */
            this.accumulatedReadings.WritePendingReadings();
        }

        private void CreateDataPoint(Reading newReading, PersistenceStrategy persistStrategy)
        {
            DateTime originalSlotExpiryTimestamp = this.slotExpiryTimestamp;

            // Push as many missing NaN data points as necessary to the queue
            PushMissingDataPoints(newReading.Timestamp);

            /*
             * Calculate the percentage of readings that are missing versus the x 
             * factor (minimum % of readings required for the current slot 
             * to produce a valid value.) If the percentage is the same or higher
             * than the x factor then push the accumulated data point into the queue. If
             * sufficient readings have not accumulated during this slot, then push a
             * NaN data point into the queue.
             */
            if (CalcAccumulatedReadingsRatioAsPercentage() >= this.template.XFactor)
            {
                /*
                 * Sufficient readings have been received so push the consolidated data point. The 
                 * data point inherits the timestamp of the final reading
                 */
                this.dataPointCircularQueue.Add(CreateConsolidationFunction().CreateAccumulatedDataPoint(newReading));
            }
            else
            {
                /*
                 * Not enough readings have been received for the current 
                 * slot, push a NaN value into the queue
                 */
                this.dataPointCircularQueue.Add(DataPoint.CreateNaN(newReading.Timestamp));
            }

            ResetAccumulatedReadings();

            if (originalSlotExpiryTimestamp == this.slotExpiryTimestamp)
            {
                CalcNextSlotExpiryTime();
            }

            if (persistStrategy != PersistenceStrategy.DelayWrite)
            {
                // Save the changes that haven't been persisted to disk
                this.dataPointCircularQueue.PersistChanges();
            }
        }

        private DateTime CalcNextSlotExpiryTime()
        {
            Debug.Assert(this.slotExpiryTimestamp != DateTime.MinValue);

            this.slotExpiryTimestamp = CalcNextSlotExpiryTime(this.slotExpiryTimestamp);
            return this.slotExpiryTimestamp;
        }

        private DateTime CalcNextSlotExpiryTime(DateTime baseTime)
        {
            DateTime nextSlotTime = baseTime;
            int totalSpanDuration = Convert.ToInt32(this.dataSource.PollingInterval.TotalSeconds) * this.template.ReadingsPerDataPoint;
            TimeSpan intervalTimespan = new TimeSpan(0, 0, totalSpanDuration);
            nextSlotTime += intervalTimespan;
            return nextSlotTime;
        }

        private IConsolidationFunction CreateConsolidationFunction()
        {
            if (this.consolidationFunction != null)
            {
                return this.consolidationFunction;
            }

            IConsolidationFunction function;

            switch (this.ConsolidationFunction)
            {
                case ArchiveTemplate.ConsolidationFunctionType.Average:
                    function = new AverageConsolidationFunction(this);
                    break;

                case ArchiveTemplate.ConsolidationFunctionType.Last:
                    function = new LastConsolidationFunction(this);
                    break;

                case ArchiveTemplate.ConsolidationFunctionType.Max:
                    function = new MaxConsolidationFunction(this);
                    break;

                case ArchiveTemplate.ConsolidationFunctionType.Min:
                    function = new MinConsolidationFunction(this);
                    break;

                default:
                    throw new ArgumentException("Bad consolidation function");
            }

            this.consolidationFunction = function;

            return this.consolidationFunction;
        }

        /// <summary>
        /// Collection of archives
        /// </summary>
        public class ArchiveCollection : IEnumerable
        {
            // The data source in which the archives are embedded
            private DataSource dataSource;

            // Archives indexed by name. Names are all converted to lowercase to aid case insensitive retrieval
            private Dictionary<string, Archive> archives = new Dictionary<string, Archive>();

            // The archive collection data access object
            private BinaryFileArchiveCollectionDao dao;

            public ArchiveCollection(DataSource dataSource)
            {
                this.dataSource = dataSource;
            }

            /// <summary>
            /// Gets the number of archives
            /// </summary>
            public int Count
            {
                get { return this.archives.Count; }
            }

            /// <summary>
            /// Add a new archive to the collection
            /// </summary>
            /// <param name="newArchive">New archive to add</param>
            public void Add(Archive newArchive)
            {
                this.archives.Add(newArchive.Template.Name.ToLower(), newArchive);
            }

            /// <summary>
            /// Returns an enumerator for the collection. Not normally called directly.
            /// </summary>
            /// <returns>Enumerator object</returns>
            public IEnumerator GetEnumerator()
            {
                return this.archives.Values.GetEnumerator();
            }

            /// <summary>
            /// Create the archive collection in the database
            /// </summary>
            public void Create()
            {
                Debug.Assert(this.dao == null);

                this.dao = new BinaryFileArchiveCollectionDao((BinaryFileDataSourceDao)this.dataSource.Dao);
                this.dao.Create(CreateDto());

                foreach (Archive archive in this.archives.Values)
                {
                    archive.Create();
                }
            }

            /// <summary>
            /// Returns the archive with the given name
            /// </summary>
            /// <param name="archiveName">Name of the archive</param>
            /// <returns>Archive or null if no archive can be found</returns>
            public Archive GetArchiveByName(string archiveName)
            {
                string archiveNameLowerCase = archiveName.ToLower();
                if (this.archives.ContainsKey(archiveNameLowerCase) != true)
                {
                    // The data source specified doesn't exist
                    throw new ArgumentException("Invalid archive name");
                }

                return this.archives[archiveNameLowerCase];
            }

            /// <summary>
            /// Returns true if the archive exists with the given name
            /// </summary>
            /// <param name="archiveName">Archive to find</param>
            /// <returns>True if found, false otherwise</returns>
            public bool Exists(string archiveName)
            {
                return this.archives.ContainsKey(archiveName.ToLower());
            }

            private ArchiveCollectionDto CreateDto()
            {
                ArchiveCollectionDto dto = new ArchiveCollectionDto();
                foreach (Archive archive in this.archives.Values)
                {
                    dto.Add(archive.CreateDto());
                }
                return dto;
            }
        }
    }
}
