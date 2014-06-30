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

    public sealed class DataPoint
    {
        private double value;
        private DateTime timestamp;
        private IDataPointDao dao;

        /// <summary>
        /// Initialises a data point
        /// </summary>
        /// <param name="value">Value</param>
        /// <param name="timestamp">Timestamp</param>
        public DataPoint(double value, DateTime timestamp)
        {
            this.Value = value;
            this.Timestamp = timestamp;
        }

        internal DataPoint()
        {
        }

        public double Value
        {
            get { return this.value; }
            set { this.value = value; }
        }

        public DateTime Timestamp
        {
            get { return this.timestamp; }
            set { this.timestamp = value; }
        }

        /// <summary>
        /// Create a NaN data point
        /// </summary>
        /// <param name="timestamp">Timestamp to be associated with the new data point</param>
        /// <returns>NaN data point</returns>
        public static DataPoint CreateNaN(DateTime timestamp)
        {
            return new DataPoint(Double.NaN, timestamp);
        }

        /// <summary>
        /// Is the object the same as the given object
        /// </summary>
        /// <param name="obj">Object to compare</param>
        /// <returns>True if the same, false otherwise</returns>
        public override bool Equals(object obj)
        {
            if (obj == null) return false;

            DataPoint t = obj as DataPoint;

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

        /// <summary>
        /// Returns a human readable string to represent a data point
        /// </summary>
        /// <returns>Human readable string</returns>
        public override string ToString()
        {
            return String.Format("Data Point V={0} T={1}", this.value, this.timestamp.ToString());
        }

        public override int GetHashCode()
        {
            return ((int)this.timestamp.Ticks ^ (int)(this.timestamp.Ticks >> 32));
        }

        internal IDataPointDao Dao
        {
            get { return this.dao; }
            set { this.dao = value; }
        }

        internal void FixupFromDto(DataPointDto dto)
        {
            this.dao = dto.Dao;
            this.value = dto.Value;
            this.timestamp = dto.Timestamp;
        }

        internal DataPointDto CreateDto()
        {
            DataPointDto dto = new DataPointDto();
            dto.Dao = this.dao;
            dto.Value = this.value;
            dto.Timestamp = this.timestamp;
            return dto;
        }

        /// <summary>
        /// Collection of data points
        /// </summary>
        public class DataPointCircularCollection : IEnumerable
        {
            private BinaryFileDataPointCircularQueueDao dao;

            // The archive the reading collection is embedded inside
            private Archive archive;

            // The data points contained in the circular collection
            private List<DataPoint> dataPoints = new List<DataPoint>();

            // Maximum size of the collection
            private int maxSize;

            // The data points scheduled to be written to the database
            private List<DataPoint> dataPointsAwaitingCreation = new List<DataPoint>();

            /// <summary>
            /// Gets the number of data points in the collection
            /// </summary>
            public int Count
            {
                get { return this.dataPoints.Count; }
            }

            /// <summary>
            /// Construct a circular collection with the given maximum size
            /// </summary>
            /// <param name="archive">Archive the collection is associated with</param>
            /// <param name="maxSize">Maximum size the circular queue can grow to</param>
            internal DataPointCircularCollection(Archive archive, int maxSize)
            {
                Debug.Assert(maxSize > 0);

                this.archive = archive;
                this.maxSize = maxSize;
                this.dataPoints.Capacity = maxSize;
            }

            /// <summary>
            /// Create the data point collection on disk
            /// </summary>
            public void Create()
            {
                this.dao = new BinaryFileDataPointCircularQueueDao((BinaryFileArchiveDao)this.archive.Dao);
                this.dao.Create(CreateDto());
            }

            /// <summary>
            /// Add a new data point to the collection
            /// </summary>
            /// <param name="newDataPoint">New data point</param>
            public void Add(DataPoint newDataPoint)
            {
                // Check to see if the collection is at capacity
                if (this.dataPoints.Count == this.maxSize)
                {
                    // A data point must the removed from the head
                    this.dataPoints.RemoveAt(0);
                }
                // Add the data point to the collection of data points to be written to the database
                this.dataPointsAwaitingCreation.Add(newDataPoint);
                this.dataPoints.Add(newDataPoint);
            }

            /// <summary>
            /// Gets an enumerator for the collection
            /// </summary>
            /// <returns>Enumerator object</returns>
            public IEnumerator GetEnumerator()
            {
                return this.dataPoints.GetEnumerator();
            }

            /// <summary>
            /// Gets the data point at the given position
            /// </summary>
            /// <param name="position">Position in the collection starting at 1</param>
            /// <returns>Data point</returns>
            public DataPoint GetAt(int position)
            {
                if (position > this.dataPoints.Count || position <= 0)
                {
                    throw new ArgumentException(
                        string.Format("Position is invalid: {0}", position)
                        );
                }

                // The circular queue uses a zero based index, position starts at one
                return this.dataPoints[position - 1];
            }

            /// <summary>
            /// Returns an array of the data points starting with the given timestamp
            /// </summary>
            /// <param name="from">Timestamp to start from</param>
            /// <returns>Array of matching data points</returns>
            public DataPoint[] FilterByTime(DateTime from)
            {
                int fromIndex = FindFirstIndex(from);

                if (fromIndex == -1)
                {
                    // Unable to find a timestamp prior to the from time
                    return new DataPoint[0];
                }

                int numDataPointsInRange = this.dataPoints.Count - fromIndex;
                Debug.Assert(numDataPointsInRange > 0);

                return this.dataPoints.GetRange(fromIndex, numDataPointsInRange).ToArray();
            }

            /// <summary>
            /// Returns an array of the data points starting with the given 
            /// timestamp and ending with the to timestamp
            /// </summary>
            /// <param name="from">Timestamp to start from</param>
            /// <param name="to">Timestamp to end</param>
            /// <returns>Array of matching data points</returns>
            public DataPoint[] FilterByTime(DateTime from, DateTime to)
            {
                if (from > to)
                {
                    // ERROR: the 'from' timestamp must be earlier than the 'to' timestamp
                    throw new ArgumentException("From timestamp filter must be before the to timestamp");
                }

                int fromIndex = FindFirstIndex(from);

                if (fromIndex == -1)
                {
                    // Unable to find a timestamp prior to the from timestamp
                    return new DataPoint[0];
                }

                int toIndex = FindLastIndex(fromIndex, to);

                if (toIndex == -1)
                {
                    // Unable to find a timestamp prior to the to timestamp
                    return new DataPoint[0];
                }

                int numDataPointsInRange = (toIndex - fromIndex) + 1;
                Debug.Assert(numDataPointsInRange > 0);

                return this.dataPoints.GetRange(fromIndex, numDataPointsInRange).ToArray();
            }

            /// <summary>
            /// Returns all of the data points as an array
            /// </summary>
            /// <returns>Array of data points</returns>
            public DataPoint[] ToArray()
            {
                return this.dataPoints.ToArray();
            }

            /// <summary>
            /// Fix up the data point collection from the given DTOs
            /// </summary>
            /// <param name="readings">Data point DTO collection</param>
            internal void FixupFromDto(DataPointCircularQueueDto dataPointQueueDto)
            {
                this.dao = dataPointQueueDto.Dao;
                this.maxSize = dataPointQueueDto.MaxDataPoints;
                foreach (DataPointDto dataPointDto in dataPointQueueDto)
                {
                    DataPoint newDataPoint = new DataPoint();
                    newDataPoint.FixupFromDto(dataPointDto);
                    this.dataPoints.Add(newDataPoint);
                }
            }

            /// <summary>
            /// Persist changes to the collection to disk
            /// </summary>
            /// <param name="archiveDao"></param>
            internal void PersistChanges()
            {
                // Create the changes to the data point queue en masse to the database
                WriteDataPointsScheduledForCreation();
            }

            internal DataPointCircularQueueDto CreateDto()
            {
                DataPointCircularQueueDto dto = new DataPointCircularQueueDto();
                dto.MaxDataPoints = this.maxSize;
                foreach (DataPoint dataPoint in this.dataPoints)
                {
                    dto.Add(dataPoint.CreateDto());
                }
                return dto;
            }

            /// <summary>
            /// Create the data points scheduled for creation in the database
            /// </summary>
            private void WriteDataPointsScheduledForCreation()
            {
                // Write the pending data points to the database
                foreach (DataPoint dp in this.dataPointsAwaitingCreation)
                {
                    this.dao.Push(dp.CreateDto());
                }

                // Reset the pending data points as they have now been written
                this.dataPointsAwaitingCreation.Clear();
            }

            private int FindFirstIndex(DateTime timestampToFind)
            {
                for (int i = 0; i < this.dataPoints.Count; i++)
                {
                    DateTime currentTimestamp = this.dataPoints[i].Timestamp;

                    if (timestampToFind <= currentTimestamp)
                        return i;
                }

                return -1;
            }

            private int FindLastIndex(int fromIndex, DateTime timestampToFind)
            {
                Debug.Assert(fromIndex != -1);
                Debug.Assert(fromIndex <= this.dataPoints.Count);

                for (int i = this.dataPoints.Count - 1; i >= fromIndex; i--)
                {
                    DateTime currentTimestamp = this.dataPoints[i].Timestamp;

                    if (timestampToFind >= currentTimestamp)
                    {
                        return i;
                    }
                }

                return -1;
            }
        }
    }
}
