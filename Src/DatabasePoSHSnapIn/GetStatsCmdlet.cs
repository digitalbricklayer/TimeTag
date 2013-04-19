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
    using Openxtra.TimeTag.Database;

    /// <summary>
    /// Retrieve TimeTag Database statistics
    /// </summary>
    [Cmdlet(VerbsCommon.Get, "TTStats",
        DefaultParameterSetName = "DBParameterSet")]
    public class GetStatsCmdlet : PSCmdlet
    {
        TimeSeriesDatabase database;

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
            get { return dbFileName; }
            set { dbFileName = value; }
        }
        private string dbFileName;

        #endregion Parameters

        #region Cmdlet Overrides

        /// <summary>
        /// Returns stats object from a TimeTag database
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
                WriteObject(this.database.Stats);
            }
            finally
            {
                this.database.Close();
            }
        }

        #endregion Overrides
    }
}
