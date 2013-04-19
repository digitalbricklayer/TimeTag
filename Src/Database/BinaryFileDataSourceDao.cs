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
    using System.IO;
    using System.Diagnostics;

    internal class BinaryFileDataSourceDao : BinaryFileDao, IDataSourceDao, IBinaryFileReaderWriter
    {
        // Archives contained inside this data source
        private BinaryFileArchiveCollectionDao archives;

        // Data source name
        private string name = String.Empty;

        // The database into which this data source is embedded
        private BinaryFileTimeSeriesDatabaseDao databaseDao;

        /// <summary>
        /// The data source's entry in the fixup table
        /// </summary>
        private BinaryFileFixup fixup;

        private DataSourceDto dto;

        // Last reading DAO
        private BinaryFileReadingDao lastReadingDao;

        // Range DAO
        private BinaryFileRangeDao rangeDao;

        // Stats DAO
        private BinaryFileDataSourceStatsDao statsDao;

        public BinaryFileDataSourceDao(BinaryFileTimeSeriesDatabaseDao databaseDao)
        {
            this.databaseDao = databaseDao;
            this.archives = new BinaryFileArchiveCollectionDao(this);
        }

        public string Name
        {
            get { return this.name; }
        }

        public BinaryFileArchiveCollectionDao Archives
        {
            get { return this.archives; }
        }

        public BinaryWriter Writer
        {
            get { return this.databaseDao.Writer; }
        }

        public BinaryReader Reader
        {
            get { return this.databaseDao.Reader; }
        }

        public BinaryFileFixup Fixup
        {
            get { return this.fixup; }
            set { this.fixup = value; }
        }

        public void Close()
        {
            Debug.Assert(this.databaseDao != null);
            this.databaseDao.Close();
        }

        public void Create(DataSourceDto dto)
        {
            this.fixup = this.databaseDao.FindDataSourceFixupByName(dto.Name);
            CreateAttributes(dto);
            this.fixup.Write();
        }

        public void UpdateLastReading(ReadingDto readingDto)
        {
            Debug.Assert(this.lastReadingDao != null);
            this.lastReadingDao.Update(readingDto);
        }

        public DataSourceDto Read()
        {
            DataSourceDto dto = this.dto = new DataSourceDto();
            this.dto.Dao = this;
            SeekPosition();
            ReadAttributes();
            if (this.statsDao == null)
            {
                this.statsDao = new BinaryFileDataSourceStatsDao(this);
            }
            this.dto.Stats = this.statsDao.Read();
            this.dto.Archives = this.archives.Read();
            this.dto = null;
            return dto;
        }

        public void SetName(string newName)
        {
            Debug.Assert(this.fixup != null);

            this.fixup.SetName(newName);
        }

        private void CreateAttributes(DataSourceDto dto)
        {
            this.name = dto.Name;

            Writer.Seek(0, SeekOrigin.End);
            this.fixup.FixupPosition = Writer.BaseStream.Position;
            Writer.Write(dto.ConversionFunction);
            Writer.Write(Convert.ToInt32(dto.PollingInterval.TotalSeconds));
            if (this.lastReadingDao == null)
            {
                this.lastReadingDao = new BinaryFileReadingDao(this);
            }
            ReadingDto lastReadingDto;
            if (dto.LastReading != null)
            {
                lastReadingDto = dto.LastReading;
            }
            else
            {
                lastReadingDto = new ReadingDto();
                lastReadingDto.Empty = true;
            }
            this.lastReadingDao.Create(lastReadingDto);
            if (this.rangeDao == null)
            {
                this.rangeDao = new BinaryFileRangeDao(this);
            }
            this.rangeDao.Create(dto.Range);
        }

        private void ReadAttributes()
        {
            this.name = this.dto.Name = this.fixup.Name;
            this.dto.ConversionFunction = Reader.ReadInt32();
            this.dto.PollingInterval = new TimeSpan(0, 0, Reader.ReadInt32());
            if (this.lastReadingDao == null)
            {
                this.lastReadingDao = new BinaryFileReadingDao(this);
            }
            this.dto.LastReading = this.lastReadingDao.Read();
            if (this.rangeDao == null)
            {
                this.rangeDao = new BinaryFileRangeDao(this);
            }
            this.dto.Range = this.rangeDao.Read();
        }

        private void SeekPosition()
        {
            Position = this.fixup.FixupPosition;
            Reader.BaseStream.Seek(Position, SeekOrigin.Begin);
        }
    }
}
