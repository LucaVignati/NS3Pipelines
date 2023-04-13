import apache_beam as beam
from apache_beam.options.pipeline_options import DirectOptions, PipelineOptions
from coreFunctions import *
from utilies import *
from plotFunctions import *


class CustomOptions(PipelineOptions):
  @classmethod
  def _add_argparse_args(cls, parser):
    parser.add_argument('project_name', type=str, help="Name of the project")
    parser.add_argument('--random-seeds', type=int, default=0, help="Number of seeds to use")
    parser.add_argument('--seeds-file', type=str, default=None, help="File containing the seeds to use")
    parser.add_argument('--n-packets', type=str, default=4500000, help="Limits the cumulative number of packets across all seeds to the specified value")
    parser.add_argument('--dont-limit-packets', action='store_true', help="Do not limit the number of packets")
    parser.add_argument('--direct-num-workers', type=int, default=1, help="Number of workers to use in DirectRunner")
    parser.add_argument('--run-simulations', action='store_true', help="Run simulations")


beam_options = CustomOptions(direct_num_workers=100, direct_running_mode='multi_threading')

args = beam_options.get_all_options()
parameters_file = args['project_name'] + "/parameters.txt"
options_file = args['project_name'] + "/options.txt"

options = parseOptionsFile(options_file)
randomSeedsNumber = int(options['randomSeeds'][0])
randomSeedsSeed = int(options['randomSeedsSeed'][0])


neptuneData = None
if "neptune_API_token" in options.keys():
  neptuneData = {"api_token": options["neptune_API_token"][0],
                 "project": options["neptune_project_name"][0]}

@beam.ptransform_fn
def GenerateParameters(pipeline, parameters_file, seeds_file, randomSeedsNumber, randomSeedsSeed, totalNumberOfPackets, dont_limit_packets):
  
  if seeds_file:
    seeds = pipeline | "Read seeds" >> beam.io.ReadFromText(args['seeds_file'])

  validSeeds = randomSeedsNumber > 0 or seeds_file is not None

  seeds = pipeline | "Generate seeds" >> beam.Create(drawRandSeeds(randomSeedsNumber, 1000, randomSeedsSeed))

  simulationParameters = (
    pipeline | "ReadParameters" >> beam.io.ReadFromText(parameters_file)
    | "ExpandParameterValues" >> beam.ParDo(ExpandParameterValues(), args['project_name'])
    | "GroupByParameter" >> beam.GroupByKey()
    | "Consolidate" >> beam.CombineGlobally(Consolidate())
    | "UpdateGlobalPrameters" >> beam.ParDo(UpdateGlobalParameters(), seeds=beam.pvalue.AsIter(seeds), valid=validSeeds)
    | "CartesianProduct" >> beam.ParDo(CartesianProduct())
    | "Reshuffle" >> beam.Reshuffle()
    | "UpdateParameters" >> beam.ParDo(UpdateParameters(), totalNumberOfPackets, dont_limit_packets)
  )
  return simulationParameters

with beam.Pipeline(options=beam_options) as pipeline:

  simulationParameters = pipeline | "GenerateParameters" >> GenerateParameters(parameters_file, args['seeds_file'], randomSeedsNumber, randomSeedsSeed, int(options['totalNumberOfPackets'][0]), args['dont_limit_packets'])

  if args['run_simulations']:
    simulationParameters = simulationParameters | "RunSimulations" >> beam.ParDo(SimulationRun(), args['project_name'], options['forced'][0] == 'yes')
    # | "Reshuffle2" >> beam.Reshuffle() \
  #   | "print" >> beam.Map(print)

  # simulationResults = simulationParameters | "ParseTranferFile" >> beam.ParDo(ParseTransferFile(), True)

#   _ = simulationResults | "plotLatency" >> beam.ParDo(plotLatency)

#   e2eLatHist = simulationResults | "calculateE2ELatency" >> beam.ParDo(calculateE2eLatHist, options['binWidth'], options['highestBin']) \
#     | "calculatePacketStatistics" >> beam.ParDo(calculatePacketStatistics, options['lateTreshold'])
  
#   _ = e2eLatHist | "plotE2ELatency" >> beam.ParDo(plotE2ELatency)

#   cumulativeLat = e2eLatHist | "calculateCumulativeLatency" >> beam.ParDo(calculateCumulativeLat)
#   _ = cumulativeLat | "plotCumulativeLatency" >> beam.ParDo(plotCumulativeLat)

#   # positions = simulationParameters | "parsePositionsFile" >> beam.ParDo(parsePositionsFile)
#   # _ = positions | "plotPositions" >> beam.ParDo(plotPositions)

single_plots = 'yes' in options['plotHistogram'][0] or 'yes' in options['plotCumulative'][0] or 'yes' in options['plotPositions'][0]

beam_options = CustomOptions(direct_running_mode='in_memory')

with beam.Pipeline(options=beam_options) as pipeline:
  simulationParameters = pipeline | "GenerateParameters" >> GenerateParameters(parameters_file, args['seeds_file'], randomSeedsNumber, randomSeedsSeed, int(options['totalNumberOfPackets'][0]), args['dont_limit_packets'])
  
  simulationResults = simulationParameters | "ParseTranferFile" >> beam.ParDo(ParseTransferFile(), True)

  if single_plots:

    # _ = simulationResults | "LogInitialDataOnNeptune" >> beam.ParDo(NeptuneLogInitialData(), neptuneData)

    # e2eLatHist = simulationResults | "calculateE2ELatency" >> beam.ParDo(calculateE2eLatHist, options['binWidth'], options['highestBin']) \
    #   | "calculatePacketStatistics" >> beam.ParDo(calculatePacketStatistics, options['lateTreshold'])

    # cumulativeLat = e2eLatHist | "calculateCumulativeLatency" >> beam.ParDo(calculateCumulativeLat) \
    #   | "logStatistics" >> beam.ParDo(NeptuneLogStatistics())

    positions = simulationParameters | "parsePositionsFile" >> beam.ParDo(parsePositionsFile)

    # if 'yes' in options['plotHistogram'][0]:
    #   _ = e2eLatHist | "plotE2ELatency" >> beam.ParDo(plotE2ELatency)

    # if 'yes' in options['plotCumulative'][0]:
    #   _ = cumulativeLat | "plotCumulativeLatency" >> beam.ParDo(plotCumulativeLat)

    if 'yes' in options['plotPositions'][0]:
      _ = positions | "plotPositions" >> beam.ParDo(plotPositions)
  
  if 'yes' in options['plotSummaryHistogram'][0] or 'yes' in options['plotSummaryCumulative'][0] or 'yes' in options['plotPacketStatistics'][0]:

    averageResults = simulationResults | "AggregateSeeds" >> beam.CombineGlobally(AggregateSeeds()) \
      | "Spread" >> beam.ParDo(spread) \
      | "UpdateSummaryParamerters" >> beam.ParDo(updateSummaryParameters, randomSeedsSeed)
    
    if neptuneData:
      averageResults = averageResults | "LogSummaryDataOnNeptune" >> beam.ParDo(NeptuneLogInitialData(), neptuneData)

    e2eLatHistAvg = averageResults | "calculateE2ELatencyAvg" >> beam.ParDo(calculateE2eLatHist, options['binWidth'], options['highestBin']) \
      | "calculatePacketStatisticsAvg" >> beam.ParDo(calculatePacketStatistics, options['lateTreshold'])

    cumulativeLat = e2eLatHistAvg | "calculateCumulativeLatencyAvg" >> beam.ParDo(calculateCumulativeLat)
    if neptuneData:
      cumulativeLat = cumulativeLat | "logSummaryStatistics" >> beam.ParDo(NeptuneLogStatistics())

    if 'yes' in options['plotSummaryCumulative'][0]:
      _ = cumulativeLat | "plotCumulativeLatencyAvg" >> beam.ParDo(plotAvgCumulativeLat)

    aggregatedResults = cumulativeLat | "AggregateNumberOfUENodes" >> beam.CombineGlobally(AggregateNUEs()) \
      | "SpreadAgain" >> beam.ParDo(spread)

    if 'yes' in options['plotSummaryHistogram'][0]:
      _ = aggregatedResults | "plotE2ESummaryLatency" >> beam.ParDo(plotSummaryE2ELatency)
    if 'yes' in options['plotSummaryCumulative'][0]:
      _ = aggregatedResults | "plotCumulativeSummaryLatency" >> beam.ParDo(plotSummaryCumulativeLat)
    if 'yes' in options['plotPacketStatistics'][0]:
      _ = aggregatedResults | "plotPacketStatistics" >> beam.ParDo(plotPacketStatistics)