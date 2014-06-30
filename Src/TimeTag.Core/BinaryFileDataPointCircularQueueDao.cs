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

    internal class BinaryFileDataPointCircularQueueDao : BinaryFileDao, IEnumerable
    {
        // Index that hasn't been initialised
        private readonly static int UnknownIndex = -1;

        // Position of the first data point (in this queue) in the database file
        private long dataPointPosition = BinaryFileDao.UnknownOffset;

        // Maximum size of the queue
        private int maxSize;

        // Number of datum stored in the archive
        private int currentSize;

        // The index of the most recently pushed data point
        private int startIndex = UnknownIndex;

        // The index of the oldest datum
        private int endIndex = UnknownIndex;

        // The data point DAOs currently contained in the queue
        private List<BinaryFileDataPointDao> dataPoints = new List<BinaryFileDataPointDao>();

        // The archive into which the data point queue is embedded
        private BinaryFileArchiveDao archiveDao;

        private DataPointCircularQueueDto dto;

        public BinaryFileDataPointCircularQueueDao(BinaryFileArchiveDao archiveDao)
        {
            this.archiveDao = archiveDao;
        }

        public BinaryWriter Writer
        {
            get { return this.archiveDao.Writer; }
        }

        public BinaryReader Reader
        {
            get { return this.archiveDao.Reader; }
        }

        public virtual void Create(DataPointCircularQueueDto dataPoints)
        {
            SeekEnd();
            CreateDataPoints(dataPoints);
        }

        public void Push(DataPointDto newDataPointDto)
        {
            if (this.startIndex == -1)
            {
                Debug.Assert(this.endIndex == -1);
                Debug.Assert(this.currentSize == 0);

                // Empty case
                this.startIndex = this.endIndex = 0;
                this.currentSize++;
            }
            else if (this.currentSize < this.maxSize)
            {
                Debug.Assert(this.startIndex == 0);

                // Not full yet case, still filling up prior to wrapping around
                this.endIndex++;
                this.currentSize++;
            }
            else
            {
                Debug.Assert(this.startIndex != this.endIndex);
                Debug.Assert(this.currentSize == this.maxSize);

                // "Full" case, may require wrap around
                this.startIndex++;
                this.endIndex++;

                // Check for wrap around of either start or end
                if (this.endIndex == this.maxSize)
                {
                    this.endIndex = 0;
                }
                else if (this.startIndex == this.maxSize)
                {
                    this.startIndex = 0;
                }
            }

            Debug.Assert(GetAt(this.endIndex) != null);

            BinaryFileDataPointDao dataPointDao = GetAt(this.endIndex);
            // The data point DAO must know where it is on disk from when it was either created or read
            Debug.Assert(dataPointDao.Position != BinaryFileDao.UnknownOffset);
            dataPointDao.Write(newDataPointDto);
            newDataPointDto.Dao = (IDataPointDao) dataPointDao;

            // Write the updated archive state back to disk
            WriteState();
        }

        public DataPointCircularQueueDto Read()
        {
            DataPointCircularQueueDto dto = this.dto = new DataPointCircularQueueDto();
            this.dto.Dao = this;
            ReadDataPoints();
            this.dto = null;
            return dto;
        }

        public IEnumerator GetEnumerator()
        {
            return this.dataPoints.GetEnumerator();
        }

        private BinaryFileDataPointDao GetAt(int index)
        {
            Debug.Assert(this.dataPoints.Count > index);
            return this.dataPoints[index];
        }

        private void ReadState()
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

            // Maximum size
            this.maxSize = this.dto.MaxDataPoints = Reader.ReadInt32();

            // Current size
            this.currentSize = Reader.ReadInt32();

            // Head index
            this.startIndex = Reader.ReadInt32();

            // Tail index
            this.endIndex = Reader.ReadInt32();
        }

        private void ReadDataPoints()
        {
            ReadState();

            BinaryFileDataPointDao[] unorderedDataPointDao = new BinaryFileDataPointDao[this.dto.MaxDataPoints];
            DataPointDto[] unorderedDataPointDto = new DataPointDto[this.dto.MaxDataPoints];

            // Read the whole array of data points into unordered list
            for (int i = 0; i < this.dto.MaxDataPoints; i++)
            {
                BinaryFileDataPointDao newDataPointDao = new BinaryFileDataPointDao(archiveDao);
                DataPointDto newDataPointDto = newDataPointDao.Read();
                newDataPointDto.Dao = (IDataPointDao) newDataPointDao;
                unorderedDataPointDao[i] = newDataPointDao;
                unorderedDataPointDto[i] = newDataPointDto;
            }

            if (this.startIndex != UnknownIndex)
            {
                // Read from the first data point to the end of the array
                for (int i = this.startIndex; i < this.dto.MaxDataPoints; i++)
                {
                    this.dataPoints.Add(unorderedDataPointDao[i]);
                    if (unorderedDataPointDto[i].Empty != true)
                    {
                        this.dto.Add(unorderedDataPointDto[i]);
                    }
                }

                // Read from the beginning of the array to the end of the wrap around
                for (int i = 0; i < this.startIndex; i++)
                {
                    this.dataPoints.Add(unorderedDataPointDao[i]);
                    if (unorderedDataPointDto[i].Empty != true)
                    {
                        this.dto.Add(unorderedDataPointDto[i]);
                    }
                }
            }
            else
            {
                for (int i = 0; i < this.dto.MaxDataPoints; i++)
                {
                    this.dataPoints.Add(unorderedDataPointDao[i]);
                }
            }
        }

        private void WriteState()
        {
            if (Position != BinaryFileDao.UnknownOffset)
            {
                Writer.Seek((int)Position, SeekOrigin.Begin);
            }
            else
            {
                Position = Writer.BaseStream.Position;
            }

            Debug.Assert(this.maxSize > 0);

            // Max size
            Writer.Write(this.maxSize);

            // Current size
            Writer.Write(this.currentSize);

            // Head index
            Writer.Write(this.startIndex);

            // Tail index
            Writer.Write(this.endIndex);
        }

        private void SeekEnd()
        {
            Writer.Seek(0, SeekOrigin.End);
        }

        private void SeekPosition()
        {
            Reader.BaseStream.Seek(Position, SeekOrigin.Begin);
        }

        private void CreateDataPoints(DataPointCircularQueueDto dto)
        {
            // Write the actual data points & any empty data points not yet used
            this.maxSize = dto.MaxDataPoints;
            WriteState();
            if (dataPointPosition != BinaryFileDao.UnknownOffset)
            {
                Writer.Seek((int)this.dataPointPosition, SeekOrigin.Begin);
            }
            else
            {
                this.dataPointPosition = Writer.BaseStream.Position;
            }

            CreateEmptyDataPoints(dto);

            // The data points that do exist, overwrite the empty data points
            foreach (DataPointDto dataPointDto in dto.DataPoints)
            {
                Push(dataPointDto);
            }
        }

        private void CreateEmptyDataPoints(DataPointCircularQueueDto dto)
        {
            Debug.Assert(dto.MaxDataPoints >= dto.DataPoints.Length);

            // Write out the balance of empty data points
            for (int i = 0; i < dto.MaxDataPoints; i++)
            {
                DataPointDto emptyDataPointDto = new DataPointDto();
                emptyDataPointDto.Empty = true;
                emptyDataPointDto.Timestamp = DateTime.Now;
                emptyDataPointDto.Value = Double.NaN;

                BinaryFileDataPointDao dataPointDao = new BinaryFileDataPointDao(this.archiveDao);
                dataPointDao.Write(emptyDataPointDto);
                this.dataPoints.Add(dataPointDao);
            }
        }
    }
}
