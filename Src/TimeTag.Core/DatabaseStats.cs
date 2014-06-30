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
    /// Represents the database reading processing statistics
    /// </summary>
    public class DatabaseStats
    {
        // Total number of readings processed
        private long total;

        // Number of readings discarded
        private long discarded;

        // Stats for each data source
        private List<DataSourceStats> dataSourceStats = new List<DataSourceStats>();

        internal DatabaseStats()
        {
        }

        /// <summary>
        /// Gets a count of the readings that have been successfully processed
        /// </summary>
        public long Total
        {
            get { return this.total; }
        }

        /// <summary>
        /// Gets a count of the readings that have been discarded
        /// </summary>
        public long Discarded
        {
            get { return this.discarded; }
        }

        internal void Add(DataSourceStats stats)
        {
            this.dataSourceStats.Add(stats);
        }

        internal void CalcTotals()
        {
            Reset();
            foreach (DataSourceStats s in this.dataSourceStats)
            {
                this.total += s.Total;
                this.discarded += s.Discarded;
            }
        }

        private void Reset()
        {
            this.total = 0;
            this.discarded = 0;
        }
    }
}
