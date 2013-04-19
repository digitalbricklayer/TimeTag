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
    /// Data transfer object for the data point circular queue class
    /// </summary>
    class DataPointCircularQueueDto : IEnumerable
    {
        private BinaryFileDataPointCircularQueueDao dao;
        private List<DataPointDto> dataPoints = new List<DataPointDto>();
        private int maxDataPoints;

        /// <summary>
        /// Data access object for the datum circular queue
        /// </summary>
        public BinaryFileDataPointCircularQueueDao Dao
        {
            get { return this.dao; }
            set { this.dao = value; }
        }

        /// <summary>
        /// Collection of data points stored in the queue
        /// </summary>
        public DataPointDto[] DataPoints
        {
            get { return this.dataPoints.ToArray(); }
        }

        /// <summary>
        /// Max number of data points to be stored in the queue
        /// </summary>
        public int MaxDataPoints
        {
            get { return this.maxDataPoints; }
            set
            {
                Debug.Assert(value > 0);
                this.maxDataPoints = value;
            }
        }

        /// <summary>
        /// Add a data point to the queue
        /// </summary>
        /// <param name="newDataPointDto">New data point to add</param>
        public void Add(DataPointDto newDataPointDto)
        {
            this.dataPoints.Add(newDataPointDto);
        }

        /// <summary>
        /// Add a collection of data points to the queue
        /// </summary>
        /// <param name="newDataPointDtos">Array of data point DTO</param>
        public void Add(DataPointDto[] newDataPointDtos)
        {
            foreach (DataPointDto dto in newDataPointDtos)
            {
                this.dataPoints.Add(dto);
            }
        }

        /// <summary>
        /// Return an array of data point DTOs
        /// </summary>
        /// <returns>Array of data point DTO</returns>
        public DataPointDto[] ToArray()
        {
            return this.dataPoints.ToArray();
        }

        public IEnumerator GetEnumerator()
        {
            return this.dataPoints.GetEnumerator();
        }
    }
}
