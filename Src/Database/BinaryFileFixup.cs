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
    using System.Diagnostics;

    internal class BinaryFileFixup : BinaryFileDao, IBinaryFileReaderWriter
    {
        // Name of the entry
        private string name = "";

        private int maxNameSize;

        // Offset from the start of the file to the database object being fixed-up
        private long fixupPosition = BinaryFileDao.UnknownOffset;

        // Name DAO
        private BinaryFileStringDao nameDao;

        // The object that knows how to read and write to/from the stream
        private IBinaryFileReaderWriter readerWriter;

        public BinaryFileFixup(IBinaryFileReaderWriter readerWriter, int maxNameSize)
        {
            this.readerWriter = readerWriter;
            this.maxNameSize = maxNameSize;
        }

        public long FixupPosition
        {
            get { return this.fixupPosition; }
            set { this.fixupPosition = value; }
        }

        public string Name
        {
            get { return this.name; }
            set { this.name = value; }
        }

        public BinaryWriter Writer
        {
            get { return this.readerWriter.Writer; }
        }

        public BinaryReader Reader
        {
            get { return this.readerWriter.Reader; }
        }

        public void Read()
        {
            // Save the position of the fix-up
            Position = Reader.BaseStream.Position;

            // Position (in the database)
            this.fixupPosition = Reader.ReadInt64();

            // Name
            if (this.nameDao == null)
            {
                this.nameDao = new BinaryFileStringDao(this, this.maxNameSize);
            }
            this.name = this.nameDao.Read();

            Debug.Assert(name.Length > 0);
        }

        public void Write()
        {
            if (Position != BinaryFileDao.UnknownOffset)
            {
                Writer.Seek((int) Position, SeekOrigin.Begin);
            }
            else
            {
                // Save the location of the fixup item
                Position = Writer.BaseStream.Position;
            }

            // Position (in the database)
            Writer.Write(fixupPosition);

            // Name
            if (this.nameDao == null)
            {
                this.nameDao = new BinaryFileStringDao(this, this.maxNameSize);
            }
            this.nameDao.Update(this.name);
        }

        public void SetName(string newName)
        {
            Debug.Assert(this.nameDao != null);

            this.nameDao.Update(newName);
        }
    }
}
