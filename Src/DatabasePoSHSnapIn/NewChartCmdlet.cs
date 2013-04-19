namespace Openxtra.TimeTag.Database.PowerShell
{
    using System;
    using System.Collections;
    using System.Collections.Generic;
    using System.Collections.ObjectModel;
    using System.Security.AccessControl;
    using System.Management.Automation;
    using System.ComponentModel;
    using System.Drawing;
    using System.Drawing.Imaging;
    using System.IO;
    using System.Globalization;
    using System.Diagnostics;
    using Openxtra.TimeTag.Database;
    using ZedGraph;

    /// <summary>
    /// This class implements the New-TTChart cmdlet
    /// </summary>
    [Cmdlet(VerbsCommon.New, "TTChart")]
    public class NewChartCmdlet: PSCmdlet
    {
        private TimeSeriesDatabase database;

        private static readonly SizeF DEFAULT_IMAGE_SIZE = new SizeF(640, 480);
        private static readonly String DEFAULT_OUTPUT_FOLDER = ".";

        #region Parameters

        /// <summary>
        /// Gets the full path and filename of the database file
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
        /// Gets the data source to graph
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
            get { return this.dataSourceName; }
            set { this.dataSourceName = value; }
        }
        private string dataSourceName;

        /// <summary>
        /// Gets the archive to graph
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
            get { return this.archiveName; }
            set { this.archiveName = value; }
        }
        private string archiveName;

        [Parameter(
            ParameterSetName = "DBParameterSet",
            ValueFromPipeline = false,
            ValueFromPipelineByPropertyName = false,
            HelpMessage = "The chart title, see get-help New-TTChart for more details.")]
        public string Title
        {
            get { return this.title; }
            set { this.title = value; }
        }
        private string title;

        [Parameter(
            ParameterSetName = "DBParameterSet",
            ValueFromPipeline = false,
            ValueFromPipelineByPropertyName = false,
            HelpMessage = "The style of chart to create, see get-help New-TTChart for more details.")]
        public string ChartStyle
        {
            get { return this.chartStyle; }
            set { this.chartStyle = value; }
        }
        private string chartStyle = "line";

        [Parameter(
            ParameterSetName = "DBParameterSet",
            ValueFromPipeline = false,
            ValueFromPipelineByPropertyName = false,
            HelpMessage = "The path to place the created image file.")]
        public string OutputDirectory
        {
            get { return this.outputDirectory; }
            set { this.outputDirectory = value; }
        }
        private string outputDirectory = DEFAULT_OUTPUT_FOLDER;

        [Parameter(
            ParameterSetName = "DBParameterSet",
            ValueFromPipeline = false,
            ValueFromPipelineByPropertyName = false,
            HelpMessage = "The image type, see get-help New-TTChart for more details.")]
        public string ImageType
        {
            get { return this.imageType; }
            set { this.imageType = value; }
        }
        private string imageType = "png";

        /// <summary>
        /// Gets the width and height of the chart
        /// </summary>
        [Parameter(
            ParameterSetName = "DBParameterSet",
            ValueFromPipeline = false,
            ValueFromPipelineByPropertyName = false,
            HelpMessage = "The width and height of the chart image to create.")]
        public string[] ImageSize
        {
            get { return this.imageSize; }
            set { this.imageSize = value; }
        }
        private string[] imageSize;


        /// <summary>
        /// Gets the Y axis label
        /// </summary>
        [Parameter(
            ParameterSetName = "DBParameterSet",
            ValueFromPipeline = false,
            ValueFromPipelineByPropertyName = false,
            HelpMessage = "The Y Axis label of the chart")]
        public string YAxisLabel
        {
            get { return this.yAxisLabel; }
            set { this.yAxisLabel = value; }
        }
        private string yAxisLabel;

        /// <summary>
        /// Gets the X axis label
        /// </summary>
        [Parameter(
            ParameterSetName = "DBParameterSet",
            ValueFromPipeline = false,
            ValueFromPipelineByPropertyName = false,
            HelpMessage = "The X Axis label of the chart")]
        public string XAxisLabel
        {
            get { return this.xAxisLabel; }
            set { this.xAxisLabel = value; }
        }
        private string xAxisLabel;

        #endregion Parameters

        #region Cmdlet Overrides

        /// <summary>
        /// For each of the requested processnames, retrieve and write
        /// the associated processes.
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

                if (this.database.DataSources.Exists(this.dataSourceName) != true)
                {
                    // ERROR: can't find the specified data source
                    WriteError(
                        new ErrorRecord(
                            new ItemNotFoundException("DataSource not found."),
                            "DataSourceNotFound", ErrorCategory.ObjectNotFound, this.dataSourceName));
                    return;
                }

                DataSource dataSource = this.database.GetDataSourceByName(this.dataSourceName);

                Debug.Assert(dataSource != null);

                if (dataSource.Archives.Exists(this.archiveName) != true)
                {
                    // ERROR: can't find the specified archive
                    WriteError(
                        new ErrorRecord(
                            new ItemNotFoundException("Archive not found"),
                            "ArchiveNotFound", ErrorCategory.ObjectNotFound, this.archiveName));
                    return;
                }

                Archive archive = dataSource.GetArchiveByName(this.archiveName);

                PointPairList readings = new PointPairList();

                // Coerce the data points into a format ZedGraph can use
                foreach (Openxtra.TimeTag.Database.DataPoint dataPoint in archive.DataPoints)
                {
                    // Convert the timestamp to a XDate compatible double
                    readings.Add(new XDate(dataPoint.Timestamp), dataPoint.Value);
                }

                CreateChart(readings, dataSourceName, archiveName);
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

        #region Helper Methods

        /// <summary>
        /// Create a ZedGraph chart.
        /// </summary>
        /// <param name="readings">Array of pairs.</param>
        /// <param name="dataSourceName">The datasource name.</param>
        /// <param name="archiveName">The archive name.</param>
        private void CreateChart(PointPairList readings, string dataSourceName, string archiveName)
        {
            SizeF size = ResolveImageSize(ImageSize);

            RectangleF rectF = new RectangleF(new PointF(0, 0), size);

            GraphPane myPane = new GraphPane(
                rectF, this.title.Length > 0? this.title: "", this.XAxisLabel, this.YAxisLabel);

            if (ChartStyle != null)
            {
                CurveItem curveItem = ResolveChartStyle(ref myPane, readings, ChartStyle.ToUpper());

                if (curveItem == null)
                    return;
            }

            myPane.XAxis.Type = AxisType.DateAsOrdinal;
            myPane.XAxis.Scale.MinAuto = true;
            myPane.XAxis.Scale.MajorStepAuto = true;
            myPane.XAxis.Scale.MaxAuto = true;

            myPane.XAxis.MajorGrid.IsVisible = true;
            myPane.YAxis.MajorGrid.IsVisible = true;
            myPane.XAxis.MajorGrid.Color = Color.LightGray;
            myPane.YAxis.MajorGrid.Color = Color.LightGray;

            myPane.Legend.Position = ZedGraph.LegendPos.Bottom;

            // Calculate the Axis Scale Ranges
            using (Graphics g = Graphics.FromImage(myPane.GetImage()))
            {
                myPane.AxisChange(g);
            }

            myPane.AxisChange();

            string fileName = dataSourceName.Replace(' ', '_') + "-" + archiveName.Replace(' ', '_');

            ImageFormat imageFormat;
            string fileExt;

            if (!CheckPath(OutputDirectory))
                return;

            if (!ResolveImageFormat(ImageType.ToUpper(), out fileExt, out imageFormat))
                return;

            myPane.GetImage().Save(OutputDirectory + "\\" + fileName + fileExt, imageFormat);

        } // CreateChart

        /// <summary>
        /// Resolve an image type to a System.Drawing.Image.ImageFormat object and file extension.
        /// </summary>
        /// <param name="imageType">A string representation of the image type to be resolved.</param>
        /// <param name="fileExt">[Out] The file extension for the image type.</param>
        /// <param name="imageFormat">[Out] The System.Drawing.Image.ImageFormat representing
        /// the image type.</param>
        /// <returns>True if the image type is valid.</returns>
        private bool ResolveImageFormat(string imageType, out string fileExt, out ImageFormat imageFormat)
        {
            StringComparer comparer = StringComparer.Create(
                CultureInfo.CurrentCulture, true);

            if (comparer.Compare(imageType, "BMP") == 0)
            {
                imageFormat = System.Drawing.Imaging.ImageFormat.Bmp;
                fileExt = ".bmp";
                return true;
            }
            else if (comparer.Compare(imageType, "EMF") == 0)
            {
                imageFormat = System.Drawing.Imaging.ImageFormat.Emf;
                fileExt = ".emf";
                return true;
            }
            else if (comparer.Compare(imageType, "EXIF") == 0)
            {
                imageFormat = System.Drawing.Imaging.ImageFormat.Exif;
                fileExt = ".exif";
                return true;
            }
            else if (comparer.Compare(imageType, "GIF") == 0)
            {
                imageFormat = System.Drawing.Imaging.ImageFormat.Gif;
                fileExt = ".gif";
                return true;
            }
            else if (comparer.Compare(imageType, "JPEG") == 0 || comparer.Compare(imageType, "JPG") == 0)
            {
                imageFormat = System.Drawing.Imaging.ImageFormat.Jpeg;
                fileExt = ".jpg";
                return true;
            }
            else if (comparer.Compare(imageType, "TIFF") == 0)
            {
                imageFormat = System.Drawing.Imaging.ImageFormat.Tiff;
                fileExt = ".tif";
                return true;
            }
            else if (comparer.Compare(imageType, "WMF") == 0)
            {
                imageFormat = System.Drawing.Imaging.ImageFormat.Wmf;
                fileExt = ".wmf";
                return true;
            }
            else if (comparer.Compare(imageType, "PNG") == 0)
            {
                imageFormat = System.Drawing.Imaging.ImageFormat.Png;
                fileExt = ".png";
                return true;
            }
            else
            {
                imageFormat = null;
                fileExt = "";

                WriteError(
                    new ErrorRecord(
                        new ArgumentException("Invalid image format specified"),
                        "InvalidImageType",
                        ErrorCategory.InvalidType,
                        imageType));

                return false;
            }
        } // ResolveImageFormat

        /// <summary>
        /// Resolve a chart type to a ZedGraph.CurveItem object.
        /// </summary>
        /// <param name="chartType">The string representation of the chart type to resolve.</param>
        /// <returns>A ZedGraph.CurveItem object representing the chart style.</returns>
        private CurveItem ResolveChartStyle(ref GraphPane myPane, PointPairList readings, string chartStyle)
        {
            StringComparer comparer = StringComparer.Create(CultureInfo.CurrentCulture, true);

            String title = dataSourceName;

            if (comparer.Compare(chartStyle, "LINE") == 0)
                return myPane.AddCurve(title, readings, Color.Red, SymbolType.None);
            else if (comparer.Compare(chartStyle, "LINEMARKED") == 0)
                return myPane.AddCurve(title, readings, Color.Red, SymbolType.XCross);
            else if (comparer.Compare(chartStyle, "BAR") == 0)
                return myPane.AddBar(title, readings, Color.Red);
            else
            {
                WriteError(
                    new ErrorRecord(
                        new ArgumentException(),
                        "InvalidChartStyle",
                        ErrorCategory.InvalidType,
                        chartStyle));
                throw new ArgumentException(String.Format("Invalid chart style: {0}", chartStyle));
            }
        } // ResolveChartStyle

        /// <summary>
        /// Check to see if the path points to a valid directory.
        /// </summary>
        /// <param name="path">The path to check.</param>
        /// <returns>True if the path is a valid directory.</returns>
        private bool CheckPath(string path)
        {
            bool result = Directory.Exists(path);

            if (!result)
            {
                WriteError(new ErrorRecord(
                    new DirectoryNotFoundException(),
                    "InvalidPath",
                    ErrorCategory.ObjectNotFound,
                    path));
            }

            return result;
        } // CheckPath

        /// <summary>
        /// Creates a System.Drawing.SizeF object. 
        /// </summary>
        /// <param name="imageSize">Array object containing the width and height.</param>
        /// <returns>System.Drawing.SizeF object representing the size of the image.</returns>
        /// <remarks>Returns a default size of 640x480 if any errors occur.</remarks>
        private SizeF ResolveImageSize(string[] imageSize)
        {
            if (imageSize != null && imageSize.Length == 2)
            {
                int width = 0;
                if (!SafeConvertInt32(out width, imageSize[0]))
                {
                    return DEFAULT_IMAGE_SIZE;
                }

                int height = 0;
                if (!SafeConvertInt32(out height, imageSize[1]))
                {
                    return DEFAULT_IMAGE_SIZE;
                }

                return new SizeF(width, height);
            }
            else
            {
                WriteVerbose("Using default image size " + DEFAULT_IMAGE_SIZE.ToString());
                
                return DEFAULT_IMAGE_SIZE;
            }
        }

        /// <summary>
        /// Method to safely convert a string representation of a number 
        /// into its Int32 equivalent
        /// </summary>
        /// <param name="number">The converted number</param>
        /// <param name="numberAsStr">String representation of the number</param>
        /// <returns>False if there is an exception.</returns>
        private bool SafeConvertInt32(out int number, string numberAsStr)
        {
            number = -1;
            try
            {
                number = Convert.ToInt32(numberAsStr);
                return true;
            }
            catch (FormatException fe)
            {
                WriteError(
                    new ErrorRecord(fe, "FormatNotValid", ErrorCategory.InvalidData, numberAsStr));
            }
            catch (OverflowException oe)
            {
                WriteError(
                    new ErrorRecord(oe, "Overflow", ErrorCategory.InvalidData, numberAsStr));
            }

            return false;
        } // SafeConvertInt32

        #endregion Helper Nethods
    }
}

