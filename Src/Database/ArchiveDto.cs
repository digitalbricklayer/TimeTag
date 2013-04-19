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
    /// Data Transfer Object for the archive
    /// </summary>
    internal class ArchiveDto
    {
        private IArchiveDao dao;
        private int sequenceNumber;
        private DateTime slotExpiryTime;
        private ReadingCollectionDto accumulatedReadings = new ReadingCollectionDto();
        private DataPointCircularQueueDto datumQueue = new DataPointCircularQueueDto();

        public ArchiveDto(int sequenceNumber)
        {
            this.sequenceNumber = sequenceNumber;
        }

        /// <summary>
        /// Gets the data access object
        /// </summary>
        public IArchiveDao Dao
        {
            get { return this.dao; }
            set { this.dao = value; }
        }

        /// <summary>
        /// Gets the archive sequence number
        /// </summary>
        public int SequenceNumber
        {
            get { return this.sequenceNumber; }
        }

        /// <summary>
        /// Gets the timestamp when the current slot expires
        /// </summary>
        public DateTime SlotExpiryTime
        {
            get { return this.slotExpiryTime; }
            set { this.slotExpiryTime = value; }
        }

        /// <summary>
        /// Gets the data transfer object of the data point queue
        /// </summary>
        public DataPointCircularQueueDto DataPointQueue
        {
            get { return this.datumQueue; }
            set { this.datumQueue = value; }
        }

        /// <summary>
        /// Gets the accumulated readings for the current slot
        /// </summary>
        public ReadingCollectionDto AccumulatedReadings
        {
            get { return this.accumulatedReadings; }
            set { this.accumulatedReadings = value; }
        }
    }
}
