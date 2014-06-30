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

    /// <summary>
    /// Data transfer object for the archive collection class
    /// </summary>
    internal class ArchiveCollectionDto : IEnumerable
    {
        private BinaryFileArchiveCollectionDao dao;
        private List<ArchiveDto> archives = new List<ArchiveDto>();

        /// <summary>
        /// Gets the Data access object for the archive collection
        /// </summary>
        public BinaryFileArchiveCollectionDao Dao
        {
            get { return this.dao; }
            set { this.dao = value; }
        }

        /// <summary>
        /// Gets the collection of archives
        /// </summary>
        public ArchiveDto[] Archives
        {
            get { return this.archives.ToArray(); }
        }

        public IEnumerator GetEnumerator()
        {
            return this.archives.GetEnumerator();
        }

        /// <summary>
        /// Add an archive to the queue
        /// </summary>
        /// <param name="dto">New archive</param>
        public void Add(ArchiveDto newArchiveDto)
        {
            this.archives.Add(newArchiveDto);
        }
    }
}
