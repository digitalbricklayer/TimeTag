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
    [Cmdlet(VerbsCommon.Get, "TTDatabase",
        DefaultParameterSetName = "DBParameterSet")]
    public class GetDatabaseCmdlet : PSCmdlet
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
                WriteObject(database);
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

        #endregion Overrides
    }
}
