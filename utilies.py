from pathlib import Path
import random
from random import randint


def getFileName(params, type):
  simTime = str(int(float(params['simTime'])))
  if "numerology" in params.keys():
    numerology = "-num" + str(params["numerology"])
  else:
    numerology = ""

  if "Summary" in type:
    numberOfueNodes = [int(n) for n in params['numberOfueNodes']]
    filenameSummaryPrefix = "r" + params['radius'] + numerology + "-t" + simTime + '-avg' + str(len(params['seed'])) + '_nUEs(' + str(min(numberOfueNodes)) + '-' + str(max(numberOfueNodes)) + ')-'
    if type == "e2eLatHistSummaryPlotFile":
      return Path(params['summaryPath'], filenameSummaryPrefix + str(params['highestBin']) + 'ms' + "_e2eLatHist.png")
    elif type == "cumulativeLatSummaryPlotFile":
      return Path(params['summaryPath'], filenameSummaryPrefix + str(params['highestBin']) + 'ms' + "_cumulativeLat.png")
    elif type == "cumulativeLatSummaryDataFile":
      return Path(params['summaryPath'], filenameSummaryPrefix + str(params['highestBin']) + 'ms' + "_cumulativeLat.txt")
    elif type == "statisticsSummaryPlotFile":
      return Path(params['summaryPath'], filenameSummaryPrefix + "_statistics.png")
  elif "trace" in type:
    if type == "trace-5G":
      dlDataSinrFile = Path("DlDataSinr_r" + params['radius'] + numerology + "-s" + str(params['seed']) + "-t" + simTime + "-.txt")
      rxPacketTraceFile = Path("RxPacketTrace_r" + params['radius'] + numerology + "-s" + str(params['seed']) + "-t" + simTime + "-.txt")
      traceFiles = [dlDataSinrFile, rxPacketTraceFile]
      return traceFiles
  else:
    dataFolder = Path(params['path'], "simulations", params['numberOfueNodes'] + " UEs", "data")
    imagesFolder = Path(params['path'], "simulations", params['numberOfueNodes'] + " UEs", "images")
    filenamePrefix = "r" + params['radius'] + numerology + "-s" + str(params['seed']) + "-t" + simTime
    # if params["scenario"] == "2" or params["scenario"] == "3":
    #   if "4G" in params["generation"]: baseStation = "eNB"
    #   else: baseStation = "gNB"
    #   filenamePrefix = baseStation + "3-" + filenamePrefix

    if type == "transferFile":
      return dataFolder.joinpath(filenamePrefix + "-_transferFile.txt")
    elif type == "positionsFile":
      return dataFolder.joinpath(filenamePrefix + "-_positions.txt")
    elif type == "e2eLatHistFile":
      return imagesFolder.joinpath(filenamePrefix + '-' + str(params['highestBin']) + "ms-_e2eLatHist.png")
    elif type == "cumulativeLatFile":
      return imagesFolder.joinpath(filenamePrefix + '-' + str(params['highestBin']) + "ms-_cumulativeLat.png")
    elif type == "positionsPlotFile":
      return imagesFolder.joinpath(filenamePrefix + "-_positions.png")
    elif type == "latencyPlotFile":
      return imagesFolder.joinpath(filenamePrefix + "-_latency.png")
    elif type == "simulationSourceFile":
      return Path('scratch/4G-5G-simulation.cc')
    elif type == "trace-5G":
      dlDataSinrFile = dataFolder.joinpath("DlDataSinr_r" + params['radius'] + numerology + "-s" + str(params['seed']) + "-t" + simTime + "-.txt")
      rxPacketTraceFile = dataFolder.joinpath("RxPacketTrace_r" + params['radius'] + numerology + "-s" + str(params['seed']) + "-t" + simTime + "-.txt")
      traceFiles = [dlDataSinrFile, rxPacketTraceFile]
      return traceFiles
  return Path()

def getTracesFileNames(params):
  traceFiles = []
  dataFolder = Path(params['path'], "simulations", params['numberOfueNodes'] + " UEs", "data")
  for file in dataFolder.glob("trace-s" + str(params['seed']) + '*'):
    traceFiles.append(str(file.absolute()))
  return traceFiles



def expandValue(value):
  if ':' in value:
    start, step, end = value.split(':')
    return list(range(int(start), int(end) + 1, int(step)))
  elif ',' in value:
    return value.split(',')
  else:
    return [value]


def drawRandSeeds(numberOfSeeds, highestSeed, seed):
  random.seed(seed)
  seeds = []
  for _ in range(numberOfSeeds):
    seeds.append(str(randint(1, highestSeed)))
  return seeds
