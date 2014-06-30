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
    using System.IO;

    /// <summary>
    /// Read and write date/time objects to a binary file
    /// </summary>
    internal class BinaryFileDateTimeDao : BinaryFileDao
    {
        // The object that knows how to write/read to/from the database
        private IBinaryFileReaderWriter readWriteable;

        public BinaryFileDateTimeDao(IBinaryFileReaderWriter readerWriterable)
        {
            this.readWriteable = readerWriterable;
        }

        public BinaryWriter Writer
        {
            get { return this.readWriteable.Writer; }
        }

        public BinaryReader Reader
        {
            get { return this.readWriteable.Reader; }
        }

        public void Create(DateTime timestampToCreate)
        {
            Write(timestampToCreate);
        }

        public void Update(DateTime timestampToUpdate)
        {
            Write(timestampToUpdate);
        }

        public DateTime Read()
        {
            if (Position != BinaryFileDao.UnknownOffset)
            {
                Reader.BaseStream.Seek(Position, SeekOrigin.Begin);
            }
            else
            {
                // Save the position of the state information
                Position = Reader.BaseStream.Position;
            }

            /*
             * The timestamp is always written to the database in UTC. It must 
             * be converted to the local time zone when retrieved
             */
            DateTime utcTimestamp = DateTime.FromBinary(Reader.ReadInt64());
            return utcTimestamp.ToLocalTime();
        }

        private void Write(DateTime timestampToCreate)
        {
            if (Position == BinaryFileDao.UnknownOffset)
            {
                // Record where the reading has been read from
                Position = Writer.BaseStream.Position;
            }
            else
            {
                // Go to the location of the reading in the file
                Writer.BaseStream.Seek(Position, SeekOrigin.Begin);
            }

            // Always write the timestamp as UTC
            DateTime utcTimestamp = timestampToCreate.ToUniversalTime();
            Writer.Write(utcTimestamp.ToBinary());
        }
    }
}
