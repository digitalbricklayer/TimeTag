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

using System.Collections;
using System.Collections.Generic;
using System.IO;
using Openxtra.TimeTag.Database;

namespace TimeTag.Core.Binary
{
    internal class BinaryFileFixupTable : BinaryFileDao, IEnumerable, IBinaryFileReaderWriter
    {
        private int maxNameSize;

        // Fixups in the table
        private List<BinaryFileFixup> fixups = new List<BinaryFileFixup>();

        // The object that knows how to read and write to/from the stream
        private IBinaryFileReaderWriter readerWriter;

        public BinaryFileFixupTable(IBinaryFileReaderWriter readerWriter, int maxNameSize)
        {
            this.readerWriter = readerWriter;
            this.maxNameSize = maxNameSize;
        }

        public void Clear()
        {
            this.fixups.Clear();
        }

        public void Add(BinaryFileFixup newFixup)
        {
            this.fixups.Add(newFixup);
        }

        public void Create(BinaryFileFixupDto[] fixups)
        {
            // Save the fixup table's position
            Position = Writer.BaseStream.Position;

            // Number of items in the fixup table
            Writer.Write(fixups.Length);

            /*
             * Write a table of fix-ups. The locations of the object being fixed 
             * up are not known yet
             */
            foreach (BinaryFileFixupDto dto in fixups)
            {
                BinaryFileFixup fixup = new BinaryFileFixup(this, this.maxNameSize);
                fixup.Name = dto.Name;
                fixup.Write();
                Add(fixup);
            }
        }

        public void Read()
        {
            Clear();

            // Save the fix-up table location
            Position = Reader.BaseStream.Position;

            int size = Reader.ReadInt32();

            for (int i = 0; i < size; i++)
            {
                BinaryFileFixup fixup = new BinaryFileFixup(this, this.maxNameSize);
                fixup.Read();
                Add(fixup);
            }
        }

        public IEnumerator GetEnumerator()
        {
            return this.fixups.GetEnumerator();
        }

        public int Count
        {
            get { return this.fixups.Count; }
        }

        public BinaryFileFixup FindByName(string name)
        {
            BinaryFileFixup availableFixup = null;
            foreach (BinaryFileFixup fixup in fixups)
            {
                if (fixup.Name == name)
                {
                    availableFixup = fixup;
                    break;
                }
            }
            return availableFixup;
        }

        public BinaryWriter Writer
        {
            get { return this.readerWriter.Writer; }
        }

        public BinaryReader Reader
        {
            get { return this.readerWriter.Reader; }
        }
    }
}
