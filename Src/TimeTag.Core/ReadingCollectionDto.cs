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

    /// <summary>
    /// Data transfer object for the reading collection class
    /// </summary>
    internal class ReadingCollectionDto : IEnumerable
    {
        private BinaryFileReadingCollectionDao dao;
        private int maxReadings;
        private List<ReadingDto> readings = new List<ReadingDto>();

        /// <summary>
        /// Get the Data access object for the reading collection
        /// </summary>
        public BinaryFileReadingCollectionDao Dao
        {
            get { return this.dao; }
            set { this.dao = value; }
        }

        /// <summary>
        /// Get the collection of readings
        /// </summary>
        public ReadingDto[] Readings
        {
            get { return this.readings.ToArray(); }
        }

        /// <summary>
        /// Maximum readings storable in the collection
        /// </summary>
        public int MaxReadings
        {
            get { return this.maxReadings; }
            set { this.maxReadings = value; }
        }

        public IEnumerator GetEnumerator()
        {
            return this.readings.GetEnumerator();
        }

        /// <summary>
        /// Add a reading to the queue
        /// </summary>
        /// <param name="dto">New reading to add</param>
        public void Add(ReadingDto newReadingDto)
        {
            this.readings.Add(newReadingDto);
        }

        /// <summary>
        /// Add a number of readings to the queue
        /// </summary>
        /// <param name="newReadingDtos">Array of reading DTO</param>
        public void Add(ReadingDto[] newReadingDtos)
        {
            foreach (ReadingDto dto in newReadingDtos)
            {
                this.readings.Add(dto);
            }
        }
    }
}
