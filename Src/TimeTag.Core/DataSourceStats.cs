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
    using System.Diagnostics;

    public class DataSourceStats : ICloneable
    {
        private BinaryFileDataSourceStatsDao dao;
        private DataSource dataSource;
        private long total;
        private long discarded;

        internal DataSourceStats(DataSource dataSource)
        {
            this.dataSource = dataSource;
        }

        /// <summary>
        /// Gets a count of the readings that have been successfully processed
        /// </summary>
        public long Total
        {
            get { return this.total; }
        }

        /// <summary>
        /// Gets a count of the readings that were discarded
        /// </summary>
        public long Discarded
        {
            get { return this.discarded; }
        }

        public object Clone()
        {
            DataSourceStats clone = new DataSourceStats(this.dataSource);
            clone.total = this.total;
            clone.discarded = this.discarded;

            return clone;
        }

        /// <summary>
        /// Create the reading stats in the database
        /// </summary>
        internal void Create()
        {
            this.dao = new BinaryFileDataSourceStatsDao((BinaryFileDataSourceDao)this.dataSource.Dao);
            this.dao.Create(CreateDto());
        }

        internal void IncrementDiscarded()
        {
            Debug.Assert(this.discarded >= 0);
            this.discarded++;
        }

        internal void IncrementTotal()
        {
            Debug.Assert(this.total >= 0);
            this.total++;
        }

        internal DataSourceStatsDto CreateDto()
        {
            DataSourceStatsDto dto = new DataSourceStatsDto();
            dto.Dao = this.dao;
            dto.Total = this.total;
            dto.Discarded = this.discarded;
            return dto;
        }

        internal void FixupFromDto(DataSourceStatsDto dto)
        {
            this.dao = dto.Dao;
            this.total = dto.Total;
            this.discarded = dto.Discarded;
        }

        internal void Update()
        {
            Debug.Assert(this.dao != null);
            this.dao.Update(CreateDto());
        }
    }
}
