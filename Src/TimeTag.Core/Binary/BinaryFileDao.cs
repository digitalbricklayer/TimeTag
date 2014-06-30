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

namespace TimeTag.Core.Binary
{
    internal abstract class BinaryFileDao
    {
        // Value used to denote an unknown offset
        public const int UnknownOffset = -1;

        // Offset (from the beginning of the file) of the database object in the database file
        private long position = UnknownOffset;

        /// <summary>
        /// Offset, from the beginning of the file, of the start of the database object
        /// </summary>
        public long Position
        {
            get { return this.position; }
            set { this.position = value; }
        }
    }
}
