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
    using System.Diagnostics;

    internal class BinaryFileDataSourceCollectionDao : BinaryFileDao, IEnumerable
    {
        private List<BinaryFileDataSourceDao> dataSourceFixupTable = new List<BinaryFileDataSourceDao>();

        public BinaryFileDataSourceCollectionDao()
        {
        }

        public IEnumerator GetEnumerator()
        {
            return this.dataSourceFixupTable.GetEnumerator();
        }

        public void Add(BinaryFileDataSourceDao newDataSourceDao)
        {
            this.dataSourceFixupTable.Add(newDataSourceDao);
        }

        public BinaryFileDataSourceDao GetDataSourceByName(string dataSourceName)
        {
            BinaryFileDataSourceDao foundDataSource = null;

            foreach (BinaryFileDataSourceDao dao in this.dataSourceFixupTable)
            {
                if (dao.Name == dataSourceName)
                {
                    foundDataSource = dao;
                    break;
                }
            }
            return foundDataSource;
        }
    }
}
