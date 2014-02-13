using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.Hadoop;
using Microsoft.Hadoop.MapReduce;
using Microsoft.Hadoop.WebClient.WebHCatClient;
using Microsoft.Hadoop.WebClient.OozieClient;
using Microsoft.Hadoop.WebClient.OozieClient.Contracts;
using System.Threading;
namespace StockAnalysis
{
    class Program
    {
        static void Main(string[] args)
        {

          
            HadoopJobConfiguration myConfig = new HadoopJobConfiguration();
            myConfig.InputPath = "/world/in/worldbank";
            myConfig.OutputFolder = "/world/out";
           
            
           

            Uri myUri = new Uri("http://localhost:50111");

            string userName = "hadoop";

            string passWord = null;

            Environment.SetEnvironmentVariable("HADOOP_HOME", @"c:\hadoop");
            Environment.SetEnvironmentVariable("Java_HOME", @"c:\hadoop\jvm");

            IHadoop myCluster = Microsoft.Hadoop.MapReduce.Hadoop.Connect(myUri, userName, passWord);
          
            MapReduceResult jobResult = myCluster.MapReduceJob.Execute<ExtractValuesForIndicatorsMapper, IndicatorsReducer>(myConfig);

            HadoopJobConfiguration myConfig2 = new HadoopJobConfiguration();
            myConfig2.InputPath = "/world/out";
            myConfig2.OutputFolder = "/world/out2";
            //MapReduceResult jobResult2 = myCluster.MapReduceJob.Execute<GroupValuesMapper, GroupValuesReducer>(myConfig2);



            int exitCode = jobResult.Info.ExitCode;

           

            string exitStatus = "Failure";

            if (exitCode == 0) exitStatus = "Success";

            exitStatus = exitCode + " (" + exitStatus + ")";

            Console.WriteLine();

            Console.Write("Exit Code = " + exitStatus);

           

        }




    }


    public class IndicatorData
    {
        public double WorstValue { get; set; }
        public double BestValue { get; set; }
        /// <summary>
        /// negative if negative influence
        /// </summary>
        public double Importance { get; set; }
        public double Value { get; set; }

        
    }
    public class Indicators
    {
        public IndicatorData EmploymentPPopulation12To25 { get; set; }
        public IndicatorData EnergyImportsPercentOfUsage { get; set; }
        public IndicatorData GdpGrowth { get; set; }
        public IndicatorData GovFinalConsumtionExpenditure { get; set; }
        public IndicatorData GrossSavingsPptGdp { get; set; }
        public IndicatorData InflationConsumerPrices { get; set; }
        public IndicatorData GrossCapitalFormation { get; set; }
        public IndicatorData HouseholdFinalConsumtionExpenditure { get; set; }
        public IndicatorData UnemploymentPptLaborForce { get; set; }
        public IndicatorData TradePptGdp { get; set; }
        public IndicatorData TotalReservesMinusGoldInUSD { get; set; }
        public IndicatorData DebtTotalRescheduled { get; set; }
        public IndicatorData TimeToResolveInsolvency { get; set; }
    }

    public class IndicatorSettings
    {
        Indicators indicator = null;
        public IndicatorSettings()
        {

        }
        public IndicatorSettings(Indicators value)
        {
            indicator = value;
        }

        public double AdjustValue(string indicator, double value, double trendStrength)
        {
           
            var data = GetIndicatorData(indicator);
            if (data == null)
            {
                return 0;
            }
            var interval = (data.BestValue - data.WorstValue);
             if(interval==0){
                 return 0;
             }
            var trendSign = Math.Sign(interval);
            var interpolatedValue = (value - data.WorstValue) / interval;
            if (Math.Abs(interpolatedValue) > 1)
            {
                interpolatedValue = Math.Sign(interpolatedValue);
            }
            double result = 0.0;
            if (Math.Abs(trendStrength)>0){
                result=(interpolatedValue * (trendStrength * trendSign)) * data.Importance;
            }else{
                result=(interpolatedValue) * data.Importance;
            }
           return result;
        }
        /// <summary>
        /// a mapping of the indicaor codes to ou properties
        /// </summary>
        /// <param name="s"></param>
        /// <returns></returns>
        public IndicatorData GetIndicatorData(string s)
        {

            switch (s)
            {
                case "FP.CPI.TOTL.ZG":
                    if (indicator != null)
                    {
                        return indicator.InflationConsumerPrices;
                    }
                    else
                    {
                        return new IndicatorData
                        {
                            WorstValue = 10,
                            BestValue = 2,
                            Importance = 0.5

                        };
                    }
                    break;
                case "SL.EMP.1524.SP.ZS":
                    if (indicator != null)
                    {
                        return indicator.EmploymentPPopulation12To25;
                    }
                    else
                    {
                        return new IndicatorData
                        {
                            WorstValue = 70,
                            BestValue = 10,
                            Importance = 0.00001,
                        };
                    }
                    break;
                case "EG.IMP.CONS.ZS":
                    if (indicator != null)
                    {
                        return indicator.EnergyImportsPercentOfUsage;
                    }
                    else
                    {
                        return new IndicatorData
                        {
                            WorstValue = 100,
                            BestValue = -100,
                            Importance = 0.1
                        };
                    }
                    break;
                case "NY.GDP.MKTP.KD.ZG":
                    if (indicator != null)
                    {
                        return indicator.GdpGrowth;
                    }
                    else
                    {
                        return new IndicatorData
                        {
                            WorstValue = 0,
                            BestValue = 10,
                            Importance = 0.7
                        };
                    }
                    break;

                case "NY.GNS.ICTR.ZS":
                    if (indicator != null)
                    {
                        return indicator.GrossSavingsPptGdp;
                    }
                    else
                    {
                        return new IndicatorData
                        {
                            WorstValue = 10,
                            BestValue = 30,
                            Importance = 0.1
                        };
                    }
                    break;
                case "NE.CON.GOVT.ZS":
                    if (indicator != null)
                    {
                        return indicator.GovFinalConsumtionExpenditure;
                    }
                    else
                    {
                        return new IndicatorData
                        {
                            WorstValue = 40,
                            BestValue = 20,
                            Importance = 0.1
                        };
                    }
                    break;
               
                case "NE.GDI.TOTL.ZS":
                    if (indicator != null)
                    {
                        return indicator.GrossCapitalFormation;
                    }
                    else
                    {
                        return new IndicatorData
                        {
                            WorstValue = 10,
                            BestValue = 30,
                            Importance = 0.1
                        };
                    }
                    break;
                case "NE.CON.PETC.ZS":
                    if (indicator != null)
                    {
                        return indicator.HouseholdFinalConsumtionExpenditure;
                    }
                    else
                    {
                        return new IndicatorData
                        {
                            WorstValue = 10,
                            BestValue = 70,
                            Importance = 0.05
                        };
                    }
                    break;

                case "FI.RES.XGLD.CD":
                    if (indicator != null)
                    {
                        return indicator.TotalReservesMinusGoldInUSD;
                    }
                    else
                    {
                        return new IndicatorData
                        {
                            WorstValue = 0,
                            BestValue = 0,
                            Importance = 0
                        };
                    }
                    break;
                case "NE.TRD.GNFS.ZS":
                    if (indicator != null)
                    {
                        return indicator.TradePptGdp;
                    }
                    else
                    {
                        return new IndicatorData
                        {
                            WorstValue = 20,
                            BestValue = 60,
                            Importance = 0.2
                        };
                    }
                    break;
                case "SL.UEM.TOTL.ZS":
                    if (indicator != null)
                    {
                        return indicator.UnemploymentPptLaborForce;
                    }
                    else
                    {
                        return new IndicatorData
                        {
                            WorstValue = 15,
                            BestValue = 5,
                            Importance = 0.7
                        };
                    }
                    break;
                case "DT.TXR.DPPG.CD":
                    if (indicator != null)
                    {
                        return indicator.DebtTotalRescheduled;
                    }
                    else
                    {
                        return new IndicatorData
                        {
                            WorstValue = 0,
                            BestValue = 0,
                            Importance = 0
                        };
                    }
                    break;
                case "IC.ISV.DURS":
                    if (indicator != null)
                    {
                        return indicator.TimeToResolveInsolvency;
                    }
                    else
                    {
                        return new IndicatorData
                        {
                            WorstValue = 5,
                            BestValue = 2,
                            Importance = 0.1
                        };
                    }
                    break;
                default: return null;
            }

        }
    }


    public class ExtractValuesForIndicatorsMapper : MapperBase
    {
        public class LineOfInputData
        {
            public LineOfInputData()
            {
                YearValues = new SortedList<int, double>();
            }
            public SortedList<int, double> YearValues { get; set; }
            public string CountryName { get; set; }
            public string CountryCode { get; set; }
            public string IndicatorCode { get; set; }

            public override string ToString()
            {
                return CountryName + '\t' + CountryCode + '\t' + IndicatorCode + '\t' + "yearsArray";
            }
        }
        public LineOfInputData ExtractValueFromLine(string inputLine)
        {
            var data = new LineOfInputData();


            var lineData = inputLine.Split(',').ToList();
            var count = 0;
            var yearNumber = 1960;//the starting year in the sample
            for (var i = 0; i < lineData.Count; i++)
            {
                lineData[i] = lineData[i].Trim();
                if (i < lineData.Count - 2)
                {//if you have a middle "
                    if (lineData[i].Length > 0 && lineData[i][0] == '"' && lineData[i][lineData[i].Length - 1] != '"')//this means the string belongs together with the next
                    {
                        lineData[i + 1] = lineData[i] + lineData[i + 1];
                        continue;
                    }
                }
                switch (count)
                {
                    case 0: data.CountryName = lineData[i];
                        break;
                    case 1: data.CountryCode = lineData[i];
                        break;//number 2 is inicator name, in which we are not interested, as we use the indicator code column
                    case 2://indicator name, we use the code
                        break;
                    case 3: data.IndicatorCode = lineData[i];
                        break;
                    default:
                        double parsedValue = 0;
                        if (Double.TryParse(lineData[i], out parsedValue))
                        {
                            data.YearValues.Add(yearNumber, parsedValue);
                        }
                        yearNumber++;
                        break;
                }
                count++;
            }
            return data;
        }

        ///returns a value between -1 and 1
        ///0 is neutral trend, -1= powerful lowering trend
        ///1 is powerful rising trend
        public double ComputeTrend(SortedList<int, double> years, int currentYear)
        {
            var consideredYears = years.Where(y => y.Key <= currentYear && y.Key > currentYear - 5).Select(y => y.Value).ToList();
            if (consideredYears.Count < 3)
            {
                return 0;
            }

            double sum = 0;
            for (var i = 0; i < consideredYears.Count - 2; i++)//don't take last year value
            {
                sum += consideredYears[i];
            }
            var avg = sum / consideredYears.Count - 1;
            double difSum = 0;
            for (var i = 0; i < consideredYears.Count - 2; i++)//don't take last year value
            {
                difSum += Math.Pow(consideredYears[i] - avg, 2);
            }
            var standardDev = Math.Sqrt(difSum / (consideredYears.Count - 1));
            var trend = (consideredYears.Last() - avg) / (standardDev); //if indicator moves one standard deviation away, it is considered very powerful trend
            if (trend > 1)
            {
                trend = 1;
            }
            else if (trend < -1)
            {
                trend = -1;
            }
            return trend;

        }
        public override void Map(string inputLine, MapperContext context)
        {
            var PROCESSED_YEAR = 2007;

            var data = ExtractValueFromLine(inputLine);
            if (data.CountryName == "Country Name")
            {
                return;
            }
            if (data.YearValues.ContainsKey(PROCESSED_YEAR))
            {
                var trend = ComputeTrend(data.YearValues, PROCESSED_YEAR);

                context.EmitKeyValue(data.IndicatorCode, data.CountryCode + '\t' + data.CountryName + '\t' + trend + '\t' + data.YearValues[PROCESSED_YEAR]);

            }

        }



    }





    public class IndicatorsReducer : ReducerCombinerBase
    {
        public class CountryScore
        {
            public string CountryName { get; set; }
            public string CountryCode { get; set; }
            public double Value { get; set; }
            public double OriginalValue { get; set; }
            public double Trend { get; set; }
        }
        public override void Reduce(string key, IEnumerable<string> lines, ReducerCombinerContext context)
        {
            double TREND_IMPORTANCE = 0;
            var countriesScores = new List<CountryScore>();
            var indicatorConf = new IndicatorSettings();
            double sum = 0;
            
            foreach (var line in lines)
            {
                var data = new CountryScore();
                var dataString = line.Split('\t');
                data.CountryName = dataString[1];
                data.CountryCode = dataString[0];
                data.Trend = Double.Parse(dataString[2]);
                data.OriginalValue = Double.Parse(dataString[3]);

                sum += data.OriginalValue;
                countriesScores.Add(data);
            }
            //smothen the values distribution along the mean by 1/3
            
                var avg = sum / countriesScores.Count();
           
             foreach (var val in countriesScores)
             {
                 val.Value = val.OriginalValue + (avg - val.OriginalValue) / 2;
                 val.Value = indicatorConf.AdjustValue(key, val.Value, val.Trend*TREND_IMPORTANCE);
                // context.EmitKeyValue(val.CountryCode, key + '\t' + val.Value + "\t val=" + val.OriginalValue);
                 context.EmitKeyValue(val.CountryCode,  val.CountryName + '\t' + val.Value + "\t" + val.OriginalValue+"\t"+key);
                
             }



        }


    }


    public class GroupValuesMapper : MapperBase
    {


        public override void Map(string inputLine, MapperContext context)
        {

            var data = inputLine.Split('\t');
            //countrycode --- name,value,originalValue
            context.EmitKeyValue(data[0], data[1] + '\t' + data[2] + '\t' + data[3] + '\t' + data[4]);
        }



    }





    public class GroupValuesReducer : ReducerCombinerBase
    {
        public override void Reduce(string key, IEnumerable<string> lines, ReducerCombinerContext context)
        {
            string[] lineData = null;
            //try
            //{
                Double sum = 0;
                string country = "n/a";
                var indDetails = "";
                foreach (var line in lines)
                {

                    lineData = line.Split('\t');
                    sum += Double.Parse(lineData[1]);
                    country = lineData[0];
                    indDetails += " " + lineData[3] + "=" + lineData[1];
                }
                context.EmitKeyValue(country + "(" + key + ")", sum.ToString() + indDetails);
            //}
            //catch (Exception ex)
            //{
            //    context.EmitLine(ex.Message);
            //}
        }
         


    }


    public class OrderMapper : MapperBase
    {


        public override void Map(string inputLine, MapperContext context)
        {
            var data = inputLine.Split('\t');
            context.EmitKeyValue((-Double.Parse(data[1])).ToString(), inputLine);//order by value descending
        }



    }





    public class OrderReducer : ReducerCombinerBase
    {
        public override void Reduce(string key, IEnumerable<string> lines, ReducerCombinerContext context)
        {
            var keyD = Double.Parse(key);
            context.EmitKeyValue((-keyD).ToString(), lines.FirstOrDefault());
        }



    }

}
