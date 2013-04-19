namespace Openxtra.TimeTag.Stats.PowerShell
{
    using System;
    using System.Collections;
    using System.Collections.Generic;
    using System.Collections.ObjectModel;
    using System.Management.Automation;
    using System.ComponentModel;
    using System.Diagnostics;
    using Openxtra.TimeTag.Database;

    /// <summary>
    /// Gets the arithmetic mean of a set of numbers
    /// </summary>
    [Cmdlet(VerbsCommon.Get, "Mean")]
    public class GetMeanCmdlet : PSCmdlet
    {
        private double aggregate = 0D;
        private int totalNumbers = 0;

        #region Parameters

        /// <summary>
        /// The numbers to be averaged
        /// </summary>
        [Parameter(
            Position = 0,
            Mandatory = false,
            ValueFromPipeline = true,
            ValueFromPipelineByPropertyName = false,
            HelpMessage = "The numbers to be averaged.")]
        [ValidateNotNullOrEmpty]
        public double[] Numbers
        {
            get { return this.numbers; }
            set { this.numbers = value; }
        }
        private double[] numbers;

        /// <summary>
        /// The data points to be averaged
        /// </summary>
        [Parameter(
            Position = 0,
            Mandatory = false,
            ValueFromPipeline = true,
            ValueFromPipelineByPropertyName = false,
            HelpMessage = "The numbers to be averaged.")]
        [ValidateNotNullOrEmpty]
        public DataPoint[] DataPoints
        {
            get { return this.dataPoints; }
            set { this.dataPoints = value; }
        }
        private DataPoint[] dataPoints;

        #endregion Parameters

        #region Cmdlet Overrides

        /// <summary>
        /// Process each batch of numbers
        /// </summary>
        protected override void ProcessRecord()
        {
            double[] sample;

            if (this.numbers != null)
            {
                sample = this.Numbers;
            }
            else
            {
                double[] sampleFromDataPoints = new double[this.dataPoints.Length];
                int counter = 0;
                foreach (DataPoint d in this.DataPoints)
                {
                    sampleFromDataPoints[counter++] = d.Value;
                }
                sample = sampleFromDataPoints;
            }

            this.aggregate += CalcAggregate(sample);
            this.totalNumbers += sample.Length;
        }

        /// <summary>
        /// Returns the arithmetic mean of all of the numbers
        /// </summary>
        protected override void EndProcessing()
        {
            WriteObject(CalcMean());
        }

        #endregion Overrides

        private double CalcMean()
        {
            return this.aggregate / this.totalNumbers;
        }

        private double CalcAggregate(double[] numbers)
        {
            double aggregate = 0D;

            foreach (double d in numbers)
            {
                aggregate += d;
            }

            return aggregate;
        }
    }
}
