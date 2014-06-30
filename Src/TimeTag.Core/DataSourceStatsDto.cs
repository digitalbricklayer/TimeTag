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

    /// <summary>
    /// Data transfer object of the DataSourceStats class
    /// </summary>
    internal class DataSourceStatsDto
    {
        private BinaryFileDataSourceStatsDao dao;
        private long total;
        private long discarded;

        /// <summary>
        /// Gets the data access object for the DTO
        /// </summary>
        public BinaryFileDataSourceStatsDao Dao
        {
            get { return this.dao; }
            set { this.dao = value; }
        }

        /// <summary>
        /// Gets the number of valid readings processed
        /// </summary>
        public long Total
        {
            get { return this.total; }
            set { this.total = value; }
        }

        /// <summary>
        /// Gets the number of invalid readings processed
        /// </summary>
        public long Discarded
        {
            get { return this.discarded; }
            set { this.discarded = value; }
        }
    }
}
