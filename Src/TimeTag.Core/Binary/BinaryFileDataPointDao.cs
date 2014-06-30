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

using System.IO;
using Openxtra.TimeTag.Database;

namespace TimeTag.Core.Binary
{
    internal class BinaryFileDataPointDao : BinaryFileDao, IDataPointDao, IBinaryFileReaderWriter
    {
        // The archive into which this datum is embedded
        private BinaryFileArchiveDao archive;

        // Timestamp DAO
        private BinaryFileDateTimeDao timestampDao;

        public BinaryFileDataPointDao(BinaryFileArchiveDao archiveDao)
        {
            this.archive = archiveDao;
        }

        public BinaryWriter Writer
        {
            get { return this.archive.Writer; }
        }

        public BinaryReader Reader
        {
            get { return this.archive.Reader; }
        }

        public void Create(DataPointDto dto)
        {
            Write(dto);
        }

        public void Write(DataPointDto dto)
        {
            if (Position == BinaryFileDao.UnknownOffset)
            {
                Writer.Seek(0, SeekOrigin.End);
                // Record where the data point has been written
                Position = Writer.BaseStream.Position;
            }
            else
            {
                // Go to the location of the datum in the file
                Writer.BaseStream.Seek((int) Position, SeekOrigin.Begin);
            }

            Writer.Write(dto.Empty);
            if (this.timestampDao == null)
            {
                this.timestampDao = new BinaryFileDateTimeDao(this);
            }
            this.timestampDao.Create(dto.Timestamp);
            Writer.Write(dto.Value);
        }

        public DataPointDto Read()
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

            DataPointDto dto = new DataPointDto();

            dto.Dao = this;

            dto.Empty = Reader.ReadBoolean();
            if (this.timestampDao == null)
            {
                this.timestampDao = new BinaryFileDateTimeDao(this);
            }
            dto.Timestamp = this.timestampDao.Read();
            dto.Value = Reader.ReadDouble();

            return dto;
        }

        public void Delete()
        {
            /*
             * Don't need to implement a delete in the binary file DAO because 
             * the data points are re-used instead of being deleted
             */
        }
    }
}
