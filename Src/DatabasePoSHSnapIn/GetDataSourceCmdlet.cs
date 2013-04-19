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
    [Cmdlet(VerbsCommon.Get, "TTDataSource",
        DefaultParameterSetName = "DBParameterSet")]
    public class GetDataSourceCmdlet : PSCmdlet
    {
        private TimeSeriesDatabase database;

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
            ParameterSetName = "GetDataSourceParameterSet",
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
        /// Gets the data source to get
        /// </summary>
        /// <remarks>The data source name is case sensitive</remarks>
        [Parameter(
            ParameterSetName = "ListDataSourceParameterSet",
            ValueFromPipeline = false,
            ValueFromPipelineByPropertyName = false,
            HelpMessage = "The data source to get")]
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

                if (this.dataSource != null)
                {
                    GetDataSource();
                }
                else
                {
                    Debug.Assert(this.list != false);
                    ListDataSources();
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

        private void GetDataSource()
        {
            if (this.database.DataSources.Exists(this.dataSource))
            {
                WriteObject(this.database.GetDataSourceByName(this.dataSource));
            }
            else
            {
                WriteError(
                    new ErrorRecord(
                        new ItemNotFoundException("DataSource not found."),
                        "DataSourceNotFound", ErrorCategory.ObjectNotFound, this.dataSource));
                return;
            }
        }

        private void ListDataSources()
        {
            foreach (DataSource dataSource in this.database.DataSources)
            {
                WriteObject(dataSource);
            }
        }

        #endregion Overrides

    }
}
