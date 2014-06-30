namespace Openxtra.TimeTag.Database
{
    using System;

    /// <summary>
    /// Factory for binary file data access objects
    /// </summary>
    internal class BinaryFileDaoFactory : IDaoFactory
    {
        /// <summary>
        /// Creates a time series database DAO capable of interacting with a binary file
        /// </summary>
        /// <param name="path">Path to the database file</param>
        /// <returns>Binary file database DAO</returns>
        public ITimeSeriesDatabaseDao CreateTimeSeriesDatabase(string path)
        {
            return new BinaryFileTimeSeriesDatabaseDao(path);
        }

        /// <summary>
        /// Creates a data source DAO capable of interacting with a binary file
        /// </summary>
        /// <param name="databaseDao">Database in which the data source exists</param>
        /// <returns>Binary file data source DAO</returns>
        public IDataSourceDao CreateDataSource(ITimeSeriesDatabaseDao databaseDao)
        {
            return new BinaryFileDataSourceDao((BinaryFileTimeSeriesDatabaseDao)databaseDao);
        }

        /// <summary>
        /// Creates an archive template DAO capable of interacting with a binary file
        /// </summary>
        /// <param name="databaseDao">Database in which the data source exists</param>
        /// <returns>Binary file archive template DAO</returns>
        public IArchiveTemplateDao CreateArchiveTemplate(ITimeSeriesDatabaseDao databaseDao)
        {
            return new BinaryFileArchiveTemplateDao((BinaryFileTimeSeriesDatabaseDao)databaseDao);
        }

        /// <summary>
        /// Creates an archive DAO capable of interacting with a binary file
        /// </summary>
        /// <param name="dataSourceDao">Data source in which the archive exists</param>
        /// <returns>Binary file archive DAO</returns>
        public IArchiveDao CreateArchive(IDataSourceDao dataSourceDao)
        {
            return new BinaryFileArchiveDao((BinaryFileDataSourceDao)dataSourceDao);
        }

        /// <summary>
        /// Creates a reading DAO capable of interacting with a binary file
        /// </summary>
        /// <param name="archiveDao">Archive in which the reading exists</param>
        /// <returns>Binary file reading DAO</returns>
        public IReadingDao CreateReading(IArchiveDao archiveDao)
        {
            return new BinaryFileReadingDao((BinaryFileArchiveDao)archiveDao);
        }

        /// <summary>
        /// Creates a data point DAO capable of interacting with a binary file
        /// </summary>
        /// <param name="archiveDao">Archive in which the reading exists</param>
        /// <returns>Binary file data point DAO</returns>
        public IDataPointDao CreateDataPoint(IArchiveDao archiveDao)
        {
            return new BinaryFileDataPointDao((BinaryFileArchiveDao)archiveDao);
        }
    }
}
