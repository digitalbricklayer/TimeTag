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
    [Cmdlet(VerbsCommon.Get, "TTArchive",
        DefaultParameterSetName = "DBParameterSet")]
    public class GetArchiveCmdlet : PSCmdlet
    {
        private TimeSeriesDatabase database;
        private DataSource dataSource;

        #region Parameters

        /// <summary>
        /// The full path and filename of the database file.
        /// </summary>
        [Parameter(
            Position = 0,
            Mandatory = true,
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
            ValueFromPipeline = false,
            ValueFromPipelineByPropertyName = false,
            HelpMessage = "The data source to get")]
        public string DataSourceName
        {
            get { return this.dataSourceName; }
            set { this.dataSourceName = value; }
        }
        private string dataSourceName;

        /// <summary>
        /// Gets the archive to get
        /// </summary>
        /// <remarks>The archive name is case sensitive</remarks>
        [Parameter(
            Mandatory = false,
            ParameterSetName = "GetArchiveParameterSet",
            ValueFromPipeline = false,
            ValueFromPipelineByPropertyName = false,
            HelpMessage = "The archive to get")]
        public string ArchiveName
        {
            get { return this.archiveName; }
            set { this.archiveName = value; }
        }
        private string archiveName;

        /// <summary>
        /// Gets the list switch
        /// </summary>
        /// <remarks>The archive name is case sensitive</remarks>
        [Parameter(
            Mandatory = false,
            ParameterSetName = "ListArchiveParameterSet",
            ValueFromPipeline = false,
            ValueFromPipelineByPropertyName = false,
            HelpMessage = "List all archive in the data source")]
        public SwitchParameter List
        {
            get { return this.list; }
            set { this.list = value; }
        }
        private bool list;

        #endregion Parameters

        #region Cmdlet Overrides

        /// <summary>
        /// Returns the specified item (items if a wildcard is used)
        /// from a TimeTag database.
        /// </summary>
        /// <remarks>Currently * is the only supported wildcard.</remarks>
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

                if (this.database.DataSources.Exists(this.dataSourceName))
                {
                    this.dataSource = this.database.GetDataSourceByName(this.dataSourceName);

                    if (this.archiveName != null)
                    {
                        GetArchive();
                    }
                    else
                    {
                        ListArchives();
                    }
                }
                else
                {
                    WriteError(
                        new ErrorRecord(
                            new ItemNotFoundException("DataSource not found."),
                            "DataSourceNotFound", ErrorCategory.ObjectNotFound, this.dataSourceName));
                }
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
        }

        private void GetArchive()
        {
            if (this.dataSource.Archives.Exists(this.archiveName))
            {
                WriteObject(this.dataSource.GetArchiveByName(this.archiveName));
            }
            else
            {
                WriteError(
                    new ErrorRecord(
                        new ItemNotFoundException("Archive not found"),
                        "ArchiveNotFound", ErrorCategory.ObjectNotFound, this.archiveName));
                return;
            }
        }

        private void ListArchives()
        {
            foreach (Archive archive in this.dataSource.Archives)
            {
                WriteObject(archive);
            }
        }

        #endregion Overrides

    }
}
