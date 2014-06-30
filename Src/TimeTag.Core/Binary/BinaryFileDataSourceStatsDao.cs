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
    internal class BinaryFileDataSourceStatsDao : BinaryFileDao
    {
        // The data source to which the stats relate
        private BinaryFileDataSourceDao parent;

        public BinaryFileDataSourceStatsDao(BinaryFileDataSourceDao dataSourceDao)
        {
            this.parent = dataSourceDao;
        }

        public BinaryWriter Writer
        {
            get { return this.parent.Writer; }
        }

        public BinaryReader Reader
        {
            get { return this.parent.Reader; }
        }

        public void Create(DataSourceStatsDto dto)
        {
            SeekEnd();
            Write(dto);
        }

        public void Update(DataSourceStatsDto dto)
        {
            Write(dto);
        }

        public DataSourceStatsDto Read()
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

            DataSourceStatsDto dto = new DataSourceStatsDto();

            dto.Total = Reader.ReadInt64();
            dto.Discarded = Reader.ReadInt64();

            dto.Dao = this;

            return dto;
        }

        private void Write(DataSourceStatsDto dto)
        {
            if (Position == BinaryFileDao.UnknownOffset)
            {
                // Record where the stats has been read from
                Position = Writer.BaseStream.Position;
            }
            else
            {
                // Go to the location of the stats in the file
                Writer.BaseStream.Seek((int)Position, SeekOrigin.Begin);
            }

            Writer.Write(dto.Total);
            Writer.Write(dto.Discarded);

            dto.Dao = this;
        }

        private void SeekEnd()
        {
            Writer.Seek(0, SeekOrigin.End);
        }
    }
}
