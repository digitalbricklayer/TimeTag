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

    internal class BinaryFileArchiveCollectionDao : BinaryFileDao, IEnumerable, IBinaryFileReaderWriter
    {
        private List<BinaryFileArchiveDao> archives = new List<BinaryFileArchiveDao>();

        private IBinaryFileReaderWriter dataSourceDao;

        public BinaryFileArchiveCollectionDao(IBinaryFileReaderWriter dataSourceDao)
        {
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

        public IEnumerator GetEnumerator()
        {
            return this.archives.GetEnumerator();
        }

        public void Create(ArchiveCollectionDto dto)
        {
            SeekEnd();

            // Number of archives
            Writer.Write(dto.Archives.Length);
        }

        public ArchiveCollectionDto Read()
        {
            ArchiveCollectionDto dto = new ArchiveCollectionDto();
            dto.Dao = this;

            SeekPosition();

            // Number of archives persisted
            int archiveCount = Reader.ReadInt32();

            for (int i = 1; i <= archiveCount; i++)
            {
                BinaryFileArchiveDao archiveDao = new BinaryFileArchiveDao((BinaryFileDataSourceDao)this.dataSourceDao);
                ArchiveDto archiveDto = archiveDao.Read(i);
                archiveDto.Dao = archiveDao;
                this.archives.Add(archiveDao);
                dto.Add(archiveDto);
            }

            return dto;
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
    }
}
