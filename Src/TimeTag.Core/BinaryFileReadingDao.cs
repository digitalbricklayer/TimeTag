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

    internal class BinaryFileReadingDao : BinaryFileDao, IReadingDao, IBinaryFileReaderWriter
    {
        // The archive into which this reading is embedded
        private IBinaryFileReaderWriter parent;

        // Timestamp DAO
        private BinaryFileDateTimeDao timestampDao;

        public BinaryFileReadingDao(IBinaryFileReaderWriter parentDao)
        {
            this.parent = parentDao;
        }

        public BinaryWriter Writer
        {
            get { return this.parent.Writer; }
        }

        public BinaryReader Reader
        {
            get { return this.parent.Reader; }
        }

        public void Create(ReadingDto dto)
        {
            Write(dto);
        }

        public void Update(ReadingDto dto)
        {
            Write(dto);
        }

        public ReadingDto Read()
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

            ReadingDto dto = new ReadingDto();

            dto.Empty = Reader.ReadBoolean();
            if (this.timestampDao == null)
            {
                this.timestampDao = new BinaryFileDateTimeDao(this);
            }
            dto.Timestamp = this.timestampDao.Read();
            dto.Value = Reader.ReadDouble();

            return dto;
        }

        private void Write(ReadingDto dto)
        {
            if (Position == BinaryFileDao.UnknownOffset)
            {
                // Record where the reading has been read from
                Position = Writer.BaseStream.Position;
            }
            else
            {
                // Go to the location of the reading in the file
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
    }
}
