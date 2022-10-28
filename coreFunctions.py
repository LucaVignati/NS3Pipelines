import apache_beam as beam
from pathlib import Path
from utilies import getFileName, expandValue
from subprocess import Popen
from itertools import product
import numpy as np

# ------------------- #
# --- PARSE FILES --- #
# ------------------- #


class ParseTransferFile(beam.DoFn):
  def process(self, params, truncate):
    data = []
    filepath = getFileName(params, "transferFile")
    simTime = float(params['simTime'])

    try:
      with open(filepath, 'r') as fp:
        # Parse the rest of the file
        packetNumber = 0
        ueId = 0
        line = fp.readline()
        while line:
          dataLine = []
          for n in line.split():
            dataLine.append(int(n))
          time = dataLine[1] / 1e6
          if (packetNumber > 1000 and packetNumber < (simTime - 1) * 1000 and time < simTime) or not truncate:
            data.append(dataLine)
          if ueId == dataLine[0]:
            packetNumber += 1
          else:
            packetNumber = 0
            ueId = dataLine[0]
          line = fp.readline()

        # Sort the data in chronological order
        # data.sort(key=lambda x: x[1])
        output = params.copy()
        output['data'] = data
        output['nPackets'] = len(data)
        yield output
    except IOError:
      if Path(filepath).is_file():
        print("Error occurred opening ", filepath)
      else:
        print(filepath, ' does not exist')


def parsePositionsFile(params):
  filepath_positions = getFileName(params, "positionsFile")
  positions = []

  try:
    with open(filepath_positions, 'r') as fp:
      line = fp.readline()
      while line:
        positions.append(line.split())
        line = fp.readline()
    output = params.copy()
    output['positions'] = positions
    yield output
  except IOError:
    print(filepath_positions, ' does not exist')


def parseOptionsFile(optionsFile):
  optionsDict = {}
  with open(optionsFile, 'r') as f:
    for line in f:
      if line[0] != '#':
        key, value = line.split(' ')
        optionsDict[key] = expandValue(value)
  return optionsDict

# ------------------------- #
# --- CALCULATE METRICS --- #
# ------------------------- #


def calculateE2eLatHist(params, binWidth, highestBins):
  binWidth = float(binWidth[0])
  highestBins = [int(i) for i in highestBins]
  # Create end to end latency vector in milliseconds
  e2eLatency = []
  for t in params['data']:
    val = t[2] / 1000
    if val > 0:
      e2eLatency.append(val)
    elif val > 2000:
      e2eLatency.append(2000)

  output = params.copy()
  output['nPackets'] = len(params['data'])
  # Store the number of received packets
  output['receivedPackets'] = len(e2eLatency)
  # Store the bin width
  output['binWidth'] = binWidth

  for highestBin in highestBins:
    # Store the information in the params dictionary
    output['highestBin'] = highestBin

    # Calculate the number of bins of the histogram
    nBins = int(highestBin / binWidth)
    output['nBins'] = nBins

    # Create the histogram from the end to end latency vector, discard the edges
    e2eLatHist , _ = np.histogram(e2eLatency, range=(0, highestBin), bins=nBins)
   
    # Store the information in the params dictionary
    output['results'] = {'e2eLatHist': e2eLatHist / int(output['nPackets'])}

    yield output


def calculateCumulativeLat(params):
  # Calculate the cumulative latency distribution
  cumulativeLat = []
  acc = 0
  for bin in params['results']['e2eLatHist']:
    acc += bin
    cumulativeLat.append(acc)

  output = params.copy()
  # Store the information in the params dictionary
  output['results']['cumulativeLat'] = np.array(cumulativeLat)
  yield output

def calculatePacketStatistics(params, lateTreshold):
  downlinkLatePackets = 0
  uplinkLatePackets = 0
  for t in params['data']:
    if t[2] == 18446744073709551615:
      uplinkLatePackets += 1
    elif t[2] > int(lateTreshold[0])*1000:
      downlinkLatePackets += 1
      
  output = params.copy()
  del output['data']
  droppedPackets = output['nPackets'] - params['receivedPackets']
  output['results']['rxPercentage'] = params['receivedPackets'] / output['nPackets']
  output['results']['droppedPercentage'] = droppedPackets / output['nPackets']
  output['results']['downlinkLatePercentage'] = downlinkLatePackets / output['nPackets']
  output['results']['uplinkLatePercentage'] = uplinkLatePackets / output['nPackets']
  del output['receivedPackets']
  yield output

# ------------------ #
# --- OPERATIONS --- #
# ------------------ #


class SimulationRun(beam.DoFn):
  def process(self, params, forced=False):
    transferFile = getFileName(params, "transferFile")
    if not transferFile.exists() or forced:
      args = ["./ns3", "run"]
      a = "4G-5G_comparison "
      for key, value in params.items():
        if value == "yes":
          a += "--" + key
        else:
          a += "--" + key + "=" + value
        a += " "
      args.append(a)
      process = Popen(args)
      try:
        process.wait()
      finally:
        process.kill()
        process.wait()
    return [params]


class ExpandParameterValues(beam.DoFn):
  def process(self, element, seeds=None):
    key, value = element.split(' ')
    if key[0] != '#':
      for val in expandValue(value):
        yield key, str(val)


class Consolidate(beam.CombineFn):
  def create_accumulator(self):
    return {}

  def add_input(self, partialDict, input):
    key, value = input
    partialDict[key] = value
    return partialDict

  def merge_accumulators(self, accumulators):
    updatedDict = {}
    for accumulator in accumulators:
      updatedDict.update(accumulator)
    return updatedDict

  def extract_output(self, globalDict):
    return globalDict

class AggregateSeeds(beam.CombineFn):
  def create_accumulator(self):
    return {}

  def add_input(self, partialDict, input):
    partialKey = ''
    for key, value in input.items():
      if key != 'seed' and key != 'data':
        partialKey += str(value)
    
    if partialKey in partialDict.keys():
      partialDict[partialKey]['seed'] += [int(input['seed'])]
      partialDict[partialKey]['data'] += input['data']
      partialDict[partialKey]['nPackets'] += input['nPackets']
    else:
      partialDict[partialKey] = input.copy()
      partialDict[partialKey]['seed'] = [int(input['seed'])]

    return partialDict

  def merge_accumulators(self, accumulators):
    updatedDict = accumulators[0]
    globalKeys = updatedDict.keys()
    for accumulator in accumulators[1:]:
      for partialKey in accumulator.keys():
        if partialKey in globalKeys:
          updatedDict[partialKey]['seed'] += accumulator[partialKey]['seed']
          updatedDict[partialKey]['seed'].sort()
          updatedDict[partialKey]['data'] += accumulator[partialKey]['data']
        else:
          updatedDict[partialKey] = accumulator[partialKey]
    return updatedDict

  def extract_output(self, globalDict):
    return globalDict

def spread(params):
  for key in params.keys():
    yield params[key] 

class AggregateNUEs(beam.CombineFn):
  def create_accumulator(self):
    return {}

  def add_input(self, partialDict, input):
    partialKey = ''
    for key, value in input.items():
      if key != 'numberOfueNodes' and key != 'results' and key != 'nPackets' and key != 'simTime':
        partialKey += str(value)
    
    if partialKey in partialDict.keys():
      partialDict[partialKey]['numberOfueNodes'] += [int(input['numberOfueNodes'])]
      partialDict[partialKey]['numberOfueNodes'].sort()
      partialDict[partialKey]['results'][str(input['numberOfueNodes'])] = input['results']
    else:
      partialDict[partialKey] = input.copy()
      partialDict[partialKey]['numberOfueNodes'] = [int(input['numberOfueNodes'])]
      partialDict[partialKey]['results'] = {input['numberOfueNodes']: input['results']}

    return partialDict

  def merge_accumulators(self, accumulators):
    updatedDict = accumulators[0]
    globalKeys = updatedDict.keys()
    for accumulator in accumulators[1:]:
      for partialKey in accumulator.keys():
        if partialKey in globalKeys:
          updatedDict[partialKey]['numberOfueNodes'] += accumulator[partialKey]['numberOfueNodes']
          updatedDict[partialKey]['results'] += accumulator[partialKey]['results']
        else:
          updatedDict[partialKey] = accumulator[partialKey]
    return updatedDict

  def extract_output(self, globalDict):
    return globalDict


class UpdateGlobalParameters(beam.DoFn):
  def process(self, params, seeds, valid):
    output = params.copy()
    if valid:
      output['seed'] = list(seeds)

    output['numberOfSeeds'] = [len(output['seed'])]

    yield output

class CartesianProduct(beam.DoFn):
  def process(self, globalDict):
    for generation in globalDict["generation"]:
      partialDict = globalDict.copy()
      partialDict["generation"] = [generation]
      if generation == "4G":
        del partialDict["numerology"]
      keys = partialDict.keys()
      vals = partialDict.values()
      for instance in product(*vals):
        yield dict(zip(keys, instance))

class UpdateParameters(beam.DoFn):
  def process(self, params, totalNPackets, dontLimitPackets):

    generation = params["generation"]
    scenario = params["scenario"]
    if generation == "5G":
      numerology = "-num" + str(params["numerology"])
    else:
      numerology = ""

    output = params.copy()
    path = Path(params['path'])
    output['path'] = str(path.parent.joinpath(generation + "-scenario" + str(scenario) + numerology + "-" + path.stem))

    folder = Path(output['path'], "simulations", params['numberOfueNodes'] + " UEs")
    folder.joinpath("data").mkdir(parents=True, exist_ok=True)
    folder.joinpath("images").mkdir(parents=True, exist_ok=True)

    discardedTime = 1.5

    numberOfSeeds = params['numberOfSeeds']
    del output['numberOfSeeds']
    numberOfueNodes = int(params['numberOfueNodes'])
    interPacketInterval = float(params['interPacketInterval'])
    simTime = int(params['simTime'])

    limitPackets = not dontLimitPackets
    if limitPackets:
      nPackets = int(
          ((int(totalNPackets) + 2000 * numberOfueNodes) / numberOfSeeds) / numberOfueNodes)
      if nPackets < 3000:
        nPackets = 3000
      output['nPackets'] = str(nPackets)
      simTime = int(nPackets * interPacketInterval)

    output['simTime'] = str(simTime + discardedTime)
    yield output

def updateSummaryParameters(params, randomSeedsSeed):
  output = params.copy()
  root_folder = Path(output['path'])
  summaryPath = root_folder.joinpath("summary-" + str(randomSeedsSeed).zfill(2))
  summaryPath.mkdir(parents=True, exist_ok=True)
  output['summaryPath'] = str(summaryPath)
  yield output


def deleteData(params):
  output = params.copy()
  output['data'] = []
  return output
