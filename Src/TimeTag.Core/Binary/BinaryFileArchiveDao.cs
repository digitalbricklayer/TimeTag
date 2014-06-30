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

using System;
using System.Diagnostics;
using System.IO;
using Openxtra.TimeTag.Database;

namespace TimeTag.Core.Binary
{
    internal class BinaryFileArchiveDao : BinaryFileDao, IArchiveDao, IBinaryFileReaderWriter
    {
        // Data point circular queue DAO
        private BinaryFileDataPointCircularQueueDao dataPointQueueDao;

        // Accumulated readings DAO
        private BinaryFileReadingCollectionDao accumulatedReadingsDao;

        // The data source into which this archive is embedded
        private BinaryFileDataSourceDao dataSourceDao;

        // Archive DTO used to accumulate the values when reading
        private ArchiveDto dto;

        // Slot expiry timestamp DAO
        private BinaryFileDateTimeDao slotExpiryTimeDao;

        public BinaryFileArchiveDao(BinaryFileDataSourceDao dataSourceDao)
        {
            this.dataPointQueueDao = new BinaryFileDataPointCircularQueueDao(this);
            this.accumulatedReadingsDao = new BinaryFileReadingCollectionDao(this);
            this.dataSourceDao = dataSourceDao;
        }

        public BinaryWriter Writer
        {
            get { return this.dataSourceDao.Writer; }
        }

        public BinaryReader Reader
        {
            get { return this.dataSourceDao.Reader; }
        }

        public void Create(ArchiveDto dto)
        {
            SeekEnd();

            WriteProperties(dto);
        }

        public void SaveExpiryTime(DateTime newExpiryTime)
        {
            Debug.Assert(this.slotExpiryTimeDao != null);
            this.slotExpiryTimeDao.Update(newExpiryTime);
        }

        public ArchiveDto Read(int sequenceNumber)
        {
            ArchiveDto dto = CreateDto(sequenceNumber);
            ReadProperties(ref dto);
            dto.AccumulatedReadings = this.accumulatedReadingsDao.Read();
            dto.DataPointQueue = this.dataPointQueueDao.Read();
            return dto;
        }

        protected virtual void ReadProperties(ref ArchiveDto dto)
        {
            this.dto = dto;
            ReadAttributes();
        }

        private ArchiveDto CreateDto(int sequenceNumber)
        {
            return new ArchiveDto(sequenceNumber);
        }

        protected virtual void WriteProperties(ArchiveDto dto)
        {
            if (Position != BinaryFileDao.UnknownOffset)
            {
                Writer.Seek((int)Position, SeekOrigin.Begin);
            }
            else
            {
                Position = Writer.BaseStream.Position;
            }

            WriteAttributes(dto);
        }

        private void ReadAttributes()
        {
            if (Position != BinaryFileDao.UnknownOffset)
            {
                Reader.BaseStream.Seek(Position, SeekOrigin.Begin);
            }
            else
            {
                // Save the position of the attribute information
                Position = Reader.BaseStream.Position;
            }

            if (this.slotExpiryTimeDao == null)
            {
                this.slotExpiryTimeDao = new BinaryFileDateTimeDao(this);
            }
            this.dto.SlotExpiryTime = this.slotExpiryTimeDao.Read();
        }

        private void SeekEnd()
        {
            Writer.Seek(0, SeekOrigin.End);
        }

        private void WriteAttributes(ArchiveDto archiveDto)
        {
            if (this.slotExpiryTimeDao == null)
            {
                this.slotExpiryTimeDao = new BinaryFileDateTimeDao(this);
            }
            this.slotExpiryTimeDao.Create(archiveDto.SlotExpiryTime);
        }
    }
}
