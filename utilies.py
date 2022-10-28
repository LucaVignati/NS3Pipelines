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
    elif type == "statisticsSummaryPlotFile":
      return Path(params['summaryPath'], filenameSummaryPrefix + "_statistics.png")
  else:
    dataFolder = Path(params['path'], "simulations", params['numberOfueNodes'] + " UEs", "data")
    imagesFolder = Path(params['path'], "simulations", params['numberOfueNodes'] + " UEs", "images")
    filenamePrefix = "r" + params['radius'] + numerology + "-s" + str(params['seed']) + "-t" + simTime
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
  return Path()


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
