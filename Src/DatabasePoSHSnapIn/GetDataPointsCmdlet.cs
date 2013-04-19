namespace Openxtra.TimeTag.Database.PowerShell
{
    using System;
    using System.Collections;
    using System.Collections.Generic;
    using System.Collections.ObjectModel;
    using System.Security.AccessControl;
    using System.Management.Automation;
    using System.ComponentModel;
    using System.Text;
    using System.IO;
    using System.Diagnostics;
    using Openxtra.TimeTag.Database;

    /// <summary>
    /// Retrieve item(s) from a TimeTag Database.
    /// </summary>
    [Cmdlet(VerbsCommon.Get, "TTDataPoints",
        DefaultParameterSetName = "DBParameterSet")]
    public class GetDataPointsCmdlet : PSCmdlet
    {
        private TimeSeriesDatabase database;

        #region Parameters

        /// <summary>
        /// The full path and filename of the database file.
        /// </summary>
        [Parameter(
            Position = 0,
            Mandatory = true,
            ParameterSetName = "DBParameterSet",
            ValueFromPipeline = false,
            ValueFromPipelineByPropertyName = false,
            HelpMessage = "The path and filename of the database to create.")]
        [ValidateNotNullOrEmpty]
        public string Database
        {
            get { return this.dbFileName; }
            set { this.dbFileName = value; }
        }
        private string dbFileName;

        /// <summary>
        /// Gets the data source to get
        /// </summary>
        /// <remarks>The data source name is case sensitive</remarks>
        [Parameter(
            Position = 1,
            Mandatory = false,
            ParameterSetName = "DBParameterSet",
            ValueFromPipeline = false,
            ValueFromPipelineByPropertyName = false,
            HelpMessage = "The data source to get")]
        public string DataSource
        {
            get { return this.dataSource; }
            set { this.dataSource = value; }
        }
        private string dataSource;

        /// <summary>
        /// Gets the archive to get
        /// </summary>
        /// <remarks>The archive name is case sensitive</remarks>
        [Parameter(
            Position = 2,
            Mandatory = false,
            ParameterSetName = "DBParameterSet",
            ValueFromPipeline = false,
            ValueFromPipelineByPropertyName = false,
            HelpMessage = "The archive to get")]
        public string Archive
        {
            get { return this.archive; }
            set { this.archive = value; }
        }
        private string archive;

        /// <summary>
        /// Gets the start time filter
        /// </summary>
        /// <remarks>The archive name is case sensitive</remarks>
        [Parameter(
            Mandatory = false,
            ParameterSetName = "DBParameterSet",
            ValueFromPipeline = false,
            ValueFromPipelineByPropertyName = false,
            HelpMessage = "Start time filter")]
        public string Start
        {
            get { return this.start; }
            set { this.start = value; }
        }
        private string start;

        /// <summary>
        /// Gets the end time filter
        /// </summary>
        [Parameter(
            Mandatory = false,
            ParameterSetName = "DBParameterSet",
            ValueFromPipeline = false,
            ValueFromPipelineByPropertyName = false,
            HelpMessage = "End time filter")]
        public string End
        {
            get { return this.end; }
            set { this.end = value; }
        }
        private string end;

        #endregion Parameters

        #region Cmdlet Overrides

        /// <summary>
        /// Gets the data points in the specified archive with the specificed filter
        /// </summary>
        protected override void ProcessRecord()
        {
            string databasePath = this.SessionState.Path.GetUnresolvedProviderPathFromPSPath(this.dbFileName);

            if (!File.Exists(databasePath))
            {
                WriteError(
                    new ErrorRecord(
                        new FileNotFoundException("TimeTag database file not found."),
                        "FileNotFound",
                        ErrorCategory.WriteError,
                        this.dbFileName));
                return;
            }

            try
            {
                this.database = TimeSeriesDatabase.Read(databasePath);

                if (this.database.DataSources.Exists(this.dataSource) != true)
                {
                    WriteError(
                        new ErrorRecord(
                            new ItemNotFoundException("Data source not found"),
                            "DataSourceNotFound", ErrorCategory.ObjectNotFound, this.dataSource));
                    return;
                }

                DataSource dataSource = this.database.GetDataSourceByName(this.dataSource);

                if (dataSource.Archives.Exists(this.archive) != true)
                {
                    WriteError(
                        new ErrorRecord(
                            new ItemNotFoundException("Archive not found"),
                            "ArchiveNotFound", ErrorCategory.ObjectNotFound, this.archive));
                    return;
                }

                DateTime from = DateTime.MinValue;

                if (this.start != null)
                {
                    from = DateTime.Parse(this.start);
                }

                DateTime to = DateTime.MaxValue;

                if (this.end != null)
                {
                    to = DateTime.Parse(this.end);
                }

                Archive archive = dataSource.GetArchiveByName(this.archive);
                WriteObject(archive.DataPoints.FilterByTime(from, to), true);
            }
            catch (Exception e)
            {
                WriteError(
                    new ErrorRecord(
                       new ArgumentException("Unknown error occured"),
                       "UnknownError", ErrorCategory.InvalidArgument, e.ToString()));
            }
            finally
            {
                this.database.Close();
            }
        } // ProcessRecord

        #endregion Overrides
    }
}
