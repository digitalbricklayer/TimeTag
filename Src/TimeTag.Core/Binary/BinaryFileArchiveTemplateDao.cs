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
    internal class BinaryFileArchiveTemplateDao : BinaryFileDao, IArchiveTemplateDao, IBinaryFileReaderWriter
    {
        // The database into which the archive template is embedded
        private BinaryFileTimeSeriesDatabaseDao databaseDao;

        // Archive template DTO used to accumulate the values when reading
        private ArchiveTemplateDto dto;

        // Fixup for the archive template
        private BinaryFileFixup fixup;

        public BinaryFileArchiveTemplateDao(BinaryFileTimeSeriesDatabaseDao databaseDao)
        {
            this.databaseDao = databaseDao;
        }

        public BinaryFileFixup Fixup
        {
            get { return this.fixup; }
            set { this.fixup = value; }
        }

        public BinaryWriter Writer
        {
            get { return this.databaseDao.Writer; }
        }

        public BinaryReader Reader
        {
            get { return this.databaseDao.Reader; }
        }

        public void Create(ArchiveTemplateDto archiveDto)
        {
            SeekEnd();

            this.Fixup = this.databaseDao.FindArchiveTemplateFixupByName(archiveDto.Name);
            this.Fixup.FixupPosition = Writer.BaseStream.Position;
            WriteProperties(archiveDto);
            this.Fixup.Write();
        }

        public ArchiveTemplateDto Read()
        {
            Position = this.Fixup.FixupPosition;
            ArchiveTemplateDto archiveDto = CreateDto();
            archiveDto.Dao = this;
            ReadProperties(ref archiveDto);
            return archiveDto;
        }

        public void SetName(string newName)
        {
            Debug.Assert(this.fixup != null);

            this.fixup.SetName(newName);
        }

        protected virtual void ReadProperties(ref ArchiveTemplateDto archiveDto)
        {
            this.dto = archiveDto;
            ReadAttributes();
        }

        private ArchiveTemplateDto CreateDto()
        {
            return new ArchiveTemplateDto();
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

            this.dto.Name = this.fixup.Name;
            this.dto.ConsolidationFunction = Reader.ReadInt32();
            this.dto.XFactor = Reader.ReadInt32();
            this.dto.ReadingsPerDataPoint = Reader.ReadInt32();
            this.dto.MaxDataPoints = Reader.ReadInt32();
        }

        private void SeekEnd()
        {
            Writer.Seek(0, SeekOrigin.End);
        }

        private void SeekPosition()
        {
            Reader.BaseStream.Seek(Position, SeekOrigin.Begin);
        }

        protected virtual void WriteProperties(ArchiveTemplateDto dto)
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

        private void WriteAttributes(ArchiveTemplateDto archiveDto)
        {
            Writer.Write(archiveDto.ConsolidationFunction);
            Writer.Write(archiveDto.XFactor);
            Writer.Write(archiveDto.ReadingsPerDataPoint);
            Writer.Write(archiveDto.MaxDataPoints);
        }
    }
}
