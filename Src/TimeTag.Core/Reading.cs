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

using TimeTag.Core.Binary;

namespace Openxtra.TimeTag.Database
{
    using System;
    using System.Collections;
    using System.Collections.Generic;
    using System.Diagnostics;

    public sealed class Reading
    {
        private double value;
        private DateTime timestamp;
        private IReadingDao dao;

        /// <summary>
        /// Initialise a new reading
        /// </summary>
        /// <param name="value">Reading value</param>
        /// <param name="timestamp">Timestamp when the Reading was recorded</param>
        public Reading(double value, DateTime timestamp)
        {
            this.Value = value;
            this.Timestamp = timestamp;
        }

        /// <summary>
        /// Default constructor used when de-serialising
        /// </summary>
        internal Reading()
        {
        }

        public double Value
        {
            get { return this.value; }
            internal set { this.value = value; }
        }

        public DateTime Timestamp
        {
            get { return this.timestamp; }
            internal set { this.timestamp = value; }
        }

        internal IReadingDao Dao
        {
            get { return this.dao; }
        }

        public override bool Equals(Object obj)
        {
            if (obj == null) return false;

            Reading t = obj as Reading;

            if (t == null) return false;

            if (this.value.CompareTo(t.value) == 0 &&
                this.timestamp.CompareTo(t.timestamp) == 0)
            {
                return true;
            }
            else
            {
                return false;
            }
        }

        public override int GetHashCode()
        {
            return ((int)this.timestamp.Ticks ^ (int)(this.timestamp.Ticks >> 32));
        }

        internal DataPoint ConvertToDataPoint()
        {
            return new DataPoint(Value, Timestamp);
        }

        internal ReadingDto CreateDto()
        {
            ReadingDto Dto = new ReadingDto();
            Dto.Dao = this.dao;
            Dto.Value = this.value;
            Dto.Timestamp = this.timestamp;
            return Dto;
        }

        internal void FixupFromDto(ReadingDto dto)
        {
            this.dao = dto.Dao;
            this.value = dto.Value;
            this.timestamp = dto.Timestamp;
        }

        /// <summary>
        /// Collection of readings
        /// </summary>
        public class ReadingCollection : IEnumerable
        {
            private BinaryFileReadingCollectionDao dao;

            // The archive the reading collection is embedded inside
            private Archive archive;

            // Readings making up the collection
            private List<Reading> readings = new List<Reading>();

            // Readings scheduled to be written to the database
            private List<Reading> readingsPendingCreation = new List<Reading>();

            // Max number of readings that can be stored in the collection
            private int maxSize;

            internal ReadingCollection()
            {
            }

            /// <summary>
            /// Return the number of readings in the collection
            /// </summary>
            public int Count
            {
                get { return this.readings.Count; }
            }

            /// <summary>
            /// Construct a new reading collection
            /// </summary>
            /// <param name="archive">Archive to which the readings belong</param>
            /// <param name="maxSize">Maximum number of readings</param>
            public ReadingCollection(Archive archive, int maxSize)
            {
                this.archive = archive;
                this.maxSize = maxSize;
            }

            /// <summary>
            /// Create the accumulated readings collection in the database
            /// </summary>
            public void Create()
            {
                this.dao = new BinaryFileReadingCollectionDao((BinaryFileArchiveDao)this.archive.Dao);
                this.dao.Create(CreateDto());
            }

            /// <summary>
            /// Add a new reading to the collection
            /// </summary>
            /// <param name="newReading">New reading</param>
            public void Add(Reading newReading)
            {
                Debug.Assert(this.readings.Count < this.maxSize);

                // The reading hasn't yet been persisted, add it to the pending list
                this.readingsPendingCreation.Add(newReading);
                this.readings.Add(newReading);
            }

            /// <summary>
            /// Return an enumerator for the collection
            /// </summary>
            /// <returns>Enumerator object</returns>
            public IEnumerator GetEnumerator()
            {
                return this.readings.GetEnumerator();
            }

            /// <summary>
            /// Get the reading at the given position (starts at 1)
            /// </summary>
            /// <param name="position">Position in the collection starting at 1</param>
            /// <returns>Reading</returns>
            public Reading GetAt(int position)
            {
                if (position > this.readings.Count || position <= 0)
                {
                    throw new ArgumentException(
                        string.Format("Position is invalid: {0}", position)
                        );
                }
                // The circular queue uses a zero based index, position starts at one
                return this.readings[position - 1];
            }

            /// <summary>
            /// Returns all of the readings as an array
            /// </summary>
            /// <returns>Array of readings</returns>
            public Reading[] ToArray()
            {
                return this.readings.ToArray();
            }

            /// <summary>
            /// Reset the reading collection
            /// </summary>
            internal void Reset()
            {
                /*
                 * Are all of the readings waiting to be persisted (in which 
                 * case there's nothing to delete from the database)
                 */
                if (this.readingsPendingCreation.Count < this.readings.Count)
                {
                    // Delete the readings that have been persisted
                    this.dao.Clear();
                }

                /*
                 * Reset the pending readings, as they've not been persisted to disk
                 * they don't need to be deleted
                 */
                this.readingsPendingCreation.Clear();

                // Clear the in-memory readings
                this.readings.Clear();
            }

            /// <summary>
            /// Fix up the reading collection from the given DTOs
            /// </summary>
            /// <param name="readingCollectionDto">Reading DTO collection</param>
            internal void FixupFromDto(ReadingCollectionDto readingCollectionDto)
            {
                this.dao = readingCollectionDto.Dao;

                foreach (ReadingDto readingDto in readingCollectionDto)
                {
                    Reading newReading = new Reading();
                    newReading.FixupFromDto(readingDto);
                    this.readings.Add(newReading);
                }
            }

            /// <summary>
            /// Write new readings to the database
            /// </summary>
            internal void WritePendingReadings()
            {
                List<ReadingDto> readingsToPersist = new List<ReadingDto>();

                foreach (Reading pendingReading in this.readingsPendingCreation)
                {
                    /*
                     * If the reading is in the pending collection it must also 
                     * be in the main collection too
                     */
                    Debug.Assert(this.readings.Contains(pendingReading));

                    readingsToPersist.Add(pendingReading.CreateDto());
                }

                if (readingsToPersist.Count <= 0) return;

                // Persist the new readings en masse
                this.dao.Update(readingsToPersist.ToArray());

                // Clean out the pending readings, they've been created now
                this.readingsPendingCreation.Clear();
            }

            internal ReadingCollectionDto CreateDto()
            {
                ReadingCollectionDto dto = new ReadingCollectionDto();
                dto.MaxReadings = this.maxSize;
                foreach (Reading reading in this.readings)
                {
                    dto.Add(reading.CreateDto());
                }
                return dto;
            }
        }
    }
}
