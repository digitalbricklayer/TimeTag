namespace Openxtra.TimeTag.Stats.PowerShell
{
    using System;
    using System.Collections;
    using System.Collections.Generic;
    using System.Collections.ObjectModel;
    using System.Management.Automation;
    using System.ComponentModel;
    using System.Diagnostics;

    /// <summary>
    /// Gets the median of a sample of numbers
    /// </summary>
    [Cmdlet(VerbsCommon.Get, "Median")]
    public class GetMedianCmdlet : PSCmdlet
    {
        private List<double> sample = new List<double>();

        #region Parameters

        /// <summary>
        /// The numbers to be averaged
        /// </summary>
        [Parameter(
            Position = 0,
            Mandatory = true,
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

        #endregion Parameters

        #region Cmdlet Overrides

        /// <summary>
        /// Process each batch of numbers
        /// </summary>
        protected override void ProcessRecord()
        {
            Debug.Assert(numbers.Length > 0);

            this.sample.AddRange(this.numbers);
        }

        /// <summary>
        /// Returns the median of the sample
        /// </summary>
        protected override void EndProcessing()
        {
            WriteObject(CalcMedian());
        }

        #endregion Overrides

        private double CalcMedian()
        {
            // Sort the range in ascending order
            this.sample.Sort();

            if (this.sample.Count % 2 == 1)
            {
                // Simple case, there is a single number in the middle
                return this.sample[this.sample.Count / 2];
            }
            else
            {
                // Not so simple case, two numbers straddle the middle
                int upperIndex = this.sample.Count / 2;

                double upperMiddle, lowerMiddle;

                upperMiddle = this.sample[upperIndex];
                lowerMiddle = this.sample[upperIndex - 1];

                // The median is a point half way between the two samples straddling the middle
                return ((upperMiddle - lowerMiddle) / 2) + lowerMiddle;
            }
        }
    }
}
