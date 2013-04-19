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

    /// <summary>
    /// Data transfer object of the Reading class
    /// </summary>
    internal class ReadingDto
    {
        private IReadingDao dao;
        private bool empty;
        private double value;
        private DateTime time;

        /// <summary>
        /// Data access object for the DTO
        /// </summary>
        public IReadingDao Dao
        {
            get { return this.dao; }
            set { this.dao = value; }
        }

        /// <summary>
        /// Flag to indicate whether the reading holds a valid value
        /// </summary>
        public bool Empty
        {
            get { return this.empty; }
            set { this.empty = value; }
        }

        /// <summary>
        /// Value of the reading
        /// </summary>
        public double Value
        {
            get { return this.value; }
            set { this.value = value; }
        }

        /// <summary>
        /// Timestamp of the reading
        /// </summary>
        public DateTime Timestamp
        {
            get { return this.time; }
            set { this.time = value; }
        }
    }
}
