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
    using System.Collections;
    using System.Collections.Generic;
    using System.IO;
    using System.Diagnostics;

    internal class BinaryFileReadingCollectionDao : BinaryFileDao, IEnumerable
    {
        // Maximum size of the collection
        private int maxSize;

        // Position of the current size field
        private long currentSizePosition;

        // Number of readings currently stored in the collection
        private int currentSize;

        // The reading DAOs currently contained in the collection
        private List<BinaryFileReadingDao> readings = new List<BinaryFileReadingDao>();

        // The readings DAOs that are currently empty
        private List<BinaryFileReadingDao> unallocatedReadings = new List<BinaryFileReadingDao>();

        // The archive into which the reading collection is embedded
        private BinaryFileArchiveDao archive;

        public BinaryFileReadingCollectionDao(BinaryFileArchiveDao archiveDao)
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

        public void Create(ReadingCollectionDto dto)
        {
            SeekEnd();

            // The collection must never grow past max size
            this.maxSize = dto.MaxReadings;

            // Write maximum size
            Writer.Write(this.maxSize);

            // Record the position of the current size field
            this.currentSizePosition = Writer.BaseStream.Position;

            // Write current size
            Writer.Write(this.currentSize);

            ReadingDto emptyReadingDto = new ReadingDto();

            // Create an empty reading DTO
            emptyReadingDto.Empty = true;
            emptyReadingDto.Value = 0D;
            emptyReadingDto.Timestamp = DateTime.Now;

            // Create empty readings in the database
            for (int i = 0; i < this.maxSize; i++)
            {
                BinaryFileReadingDao emptyReadingDao = new BinaryFileReadingDao(archive);

                emptyReadingDao.Create(emptyReadingDto);
                this.unallocatedReadings.Add(emptyReadingDao);
            }
        }

        public ReadingCollectionDto Read()
        {
            ReadingCollectionDto dto = new ReadingCollectionDto();
            dto.Dao = this;

            SeekPosition();

            // Maximum size
            dto.MaxReadings = this.maxSize = Reader.ReadInt32();

            // Record the position of the current size field
            this.currentSizePosition = Reader.BaseStream.Position;

            // Current size
            this.currentSize = Reader.ReadInt32();

            int counter = 0;

            // Read all of the non-empty readings
            for (; counter < this.currentSize; counter++)
            {
                BinaryFileReadingDao newReadingDao = new BinaryFileReadingDao(archive);
                ReadingDto newReadingDto = newReadingDao.Read();
                newReadingDto.Dao = (IReadingDao)newReadingDao;
                this.readings.Add(newReadingDao);
                dto.Add(newReadingDto);
            }

            // Read all of the empty readings
            for (; counter < this.maxSize; counter++)
            {
                BinaryFileReadingDao newReadingDao = new BinaryFileReadingDao(archive);
                ReadingDto newReadingDto = newReadingDao.Read();
                Debug.Assert(newReadingDto.Empty);
                newReadingDto.Dao = (IReadingDao)newReadingDao;
                this.unallocatedReadings.Add(newReadingDao);
            }

            return dto;
        }

        public void Update(ReadingDto[] newReadings)
        {
            Debug.Assert(newReadings.Length <= this.maxSize);

            foreach (ReadingDto reading in newReadings)
            {
                IReadingDao dao = AllocateReading();
                dao.Update(reading);
            }

            UpdateCurrentSize();
        }

        public void Clear()
        {
            Debug.Assert(this.readings.Count + this.unallocatedReadings.Count == this.maxSize);

            this.unallocatedReadings.AddRange(this.readings.ToArray());
            
            this.readings.Clear();
            this.currentSize = 0;
            UpdateCurrentSize();

            Debug.Assert(this.unallocatedReadings.Count == this.maxSize);
            Debug.Assert(this.readings.Count == 0);
            Debug.Assert(this.currentSize == 0);
        }

        public IEnumerator GetEnumerator()
        {
            return this.readings.GetEnumerator();
        }

        private void SeekEnd()
        {
            Writer.Seek(0, SeekOrigin.End);

            // Record the location of the collection in the file
            Position = Writer.BaseStream.Position;
        }

        private void SeekPosition()
        {
            if (Position != BinaryFileDao.UnknownOffset)
            {
                Reader.BaseStream.Seek(Position, SeekOrigin.Begin);
            }
            else
            {
                Position = Reader.BaseStream.Position;
            }
        }

        private void UpdateCurrentSize()
        {
            Debug.Assert(this.currentSizePosition > 0);

            Writer.BaseStream.Seek(this.currentSizePosition, SeekOrigin.Begin);
            Writer.Write(this.currentSize);
        }

        private IReadingDao AllocateReading()
        {
            Debug.Assert(this.currentSize < this.maxSize);

            BinaryFileReadingDao readingDao = this.unallocatedReadings[0];

            // Remove an unallocated reading from the unallocated list
            this.unallocatedReadings.RemoveAt(0);

            // Add the reading DAO to the allocated list
            this.readings.Add(readingDao);

            this.currentSize++;

            return readingDao;
        }
    }
}
