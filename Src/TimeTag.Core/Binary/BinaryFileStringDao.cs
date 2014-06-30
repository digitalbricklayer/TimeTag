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

using System.Diagnostics;
using System.IO;
using Openxtra.TimeTag.Database;

namespace TimeTag.Core.Binary
{
    /// <summary>
    /// Read and write strings to a binary file
    /// </summary>
    internal class BinaryFileStringDao : BinaryFileDao
    {
        private byte maxSize;

        private long stringPosition = BinaryFileDao.UnknownOffset;

        // The object that knows how to write/read to/from the database
        private IBinaryFileReaderWriter readWriteable;

        public BinaryFileStringDao(IBinaryFileReaderWriter readerWriterable, int maxSize)
        {
            this.readWriteable = readerWriterable;
            Debug.Assert(maxSize <= byte.MaxValue);
            this.maxSize = System.Convert.ToByte(maxSize);
        }

        public BinaryWriter Writer
        {
            get { return this.readWriteable.Writer; }
        }

        public BinaryReader Reader
        {
            get { return this.readWriteable.Reader; }
        }

        public void Create(string stringToCreate)
        {
            Write(stringToCreate);
        }

        public void Update(string stringToUpdate)
        {
            Write(stringToUpdate);
        }

        public string Read()
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

            // Read the max size
            this.maxSize = Reader.ReadByte();

            if (this.stringPosition == BinaryFileDao.UnknownOffset)
            {
                // Record where the reading has been read from
                this.stringPosition = Reader.BaseStream.Position;
            }
            else
            {
                // Go to the location of the reading in the file
                Reader.BaseStream.Seek(this.stringPosition, SeekOrigin.Begin);
            }

            string stringToRead = Reader.ReadString();

            if (stringToRead.Length < this.maxSize)
            {
                /*
                 * After reading the string from the stream, the current stream position is located 
                 * at the byte after the end of the string. Given that the string written may not
                 * be as large as the space allocated in the stream, the position marker must be 
                 * moved to the start of the next field.
                 */
                Reader.BaseStream.Seek(this.maxSize - stringToRead.Length, SeekOrigin.Current);
            }
            return stringToRead;
        }

        private void Write(string stringToCreate)
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

            // Write the maximum size of the string allocated on disk
            Writer.Write(this.maxSize);

            if (this.stringPosition == BinaryFileDao.UnknownOffset)
            {
                // Record where the reading has been read from
                this.stringPosition = Writer.BaseStream.Position;
            }
            else
            {
                // Go to the location of the reading in the file
                Writer.BaseStream.Seek(this.stringPosition, SeekOrigin.Begin);
            }
            Writer.Write(stringToCreate);

            if (stringToCreate.Length < this.maxSize)
            {
                // Write filler
                byte numBytesOfFiller = System.Convert.ToByte(this.maxSize - stringToCreate.Length);

                Debug.Assert(numBytesOfFiller < this.maxSize);

                for (int i = 0; i < numBytesOfFiller; i++)
                {
                    // Write dummy data, it is not read
                    Writer.Write((byte)0);
                }
            }
        }
    }
}
