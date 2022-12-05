from pathlib import Path
from utilies import getFileName
import matplotlib
matplotlib.use('Agg')
from matplotlib import pyplot as plt
import numpy as np
from itertools import cycle
import neptune.new as neptune
from neptune.new.types import File

def uploadFigureToNeptune(params, name, fig=None, value=None, step=None, is_picture=False):
    run = neptune.init_run(with_id=params['run'],
                               api_token=params['api_token'],
                               project=params['project'])
    if is_picture:
        run[name].upload(File.as_html(fig))
    else:
        for val, st in zip(value, step):
            run[name].log(value=val, step=st)
    run.stop()

def plotE2ELatency(params, save=True, rxInLegend=True, forced=True):
    """
    Plots the end to end latency histogram
    """
    e2eLatHist = params['results']['e2eLatHist']
    e2eLatPlotFile = getFileName(params, 'e2eLatHistFile')
    if not e2eLatPlotFile.exists() or forced:
        fig, ax = plt.subplots()
        legendString = str(params['numberOfueNodes']) + ' UEs'
        if rxInLegend:
            rxString = ' - Rx Packets: ' + str(round(params['results']['rxPercentage'] * 100, 3)) + '%'
        else:
            rxString = ''
        legendString += rxString
        xLabel = 'Latency [ms]'
        yLabel = 'Probability [%]'
        plotHistogram(ax, params['highestBin'], params['nBins'], params['binWidth'], e2eLatHist, legendString, xLabel, yLabel)
        if params['neptune']:
            uploadFigureToNeptune(params, 'e2eLatHist', fig=fig, is_picture=True)
        if save:
            fig.savefig(str(e2eLatPlotFile), bbox_inches='tight')
        plt.close(fig)

def plotSummaryE2ELatency(params, save=True):
    fig, ax = plt.subplots()
    legendStrings = []
    xLabel = 'Latency [ms]'
    yLabel = 'Probability [%]'
    e2eLatPlotFile = str(getFileName(params, 'e2eLatHistSummaryPlotFile'))
    for numberOfueNodes, results in params['results'].items():
        e2eLatHist = results['e2eLatHist']
        legendStrings.append(numberOfueNodes + ' UEs')
        ax = plotHistogram(ax, params['highestBin'], params['nBins'], params['binWidth'], e2eLatHist, '', xLabel, yLabel)
    
    ax.legend(legendStrings)
    if params['neptune']:
        uploadFigureToNeptune(params, 'e2eLatHist', fig=fig, is_picture=True)
    if save:
        fig.savefig(e2eLatPlotFile, bbox_inches='tight')
    plt.close(fig)

def plotHistogram(ax, highestBin, nBins, binWidth, hist, legendString, xLabel, yLabel):
    ax.bar(np.linspace(0, highestBin, nBins), hist, width=binWidth)
    ax.set_xlabel(xLabel)
    ax.set_ylabel(yLabel)
    ax.grid(axis='y', alpha=0.75)
    ax.legend([legendString])
    return ax

def plotCumulativeLat(params, save=True, forced=True):
    """
    Plots the cumulative latency distribution
    """
    cumulativeLat = params['results']['cumulativeLat']
    e2eCumLatPlotFile = getFileName(params, 'cumulativeLatFile')
    if not e2eCumLatPlotFile.exists() or forced:
        fig, ax = plt.subplots()
        xLabel = 'Latency [ms]'
        yLabel = 'CDF [Latency]'
        legendString = str(params['numberOfueNodes']) + ' UEs - Lost (downlink): ' + str(round(params['results']['droppedPercentage'] * 100, 3)) + '%\n' + \
                                                        'Late + lost (uplink)' + str(round(params['results']['uplinkLatePercentage'] * 100, 3)) + '%\n' + \
                                                        'Late (downlink)' + str(round(params['results']['downlinkLatePercentage'] * 100, 3)) + '%\n'
        plotCumulative(ax, params['highestBin'], params['nBins'], cumulativeLat, xLabel, yLabel, legendString)
        if params['neptune']:
            uploadFigureToNeptune(params, 'cumulativeLat', value=cumulativeLat, step=np.linspace(0, params['highestBin'], params['nBins']))
        if save:
            fig.savefig(str(e2eCumLatPlotFile), bbox_inches='tight')
        plt.close(fig)

def plotSummaryCumulativeLat(params, save=True):
    fig, ax = plt.subplots()
    legendStrings = []
    xLabel = 'Latency [ms]'
    yLabel = 'CDF [Latency]'
    e2eCumLatPlotFile = str(getFileName(params, 'cumulativeLatSummaryPlotFile'))
    for numberOfueNodes, results in params['results'].items():
        cumulativeLat = results['cumulativeLat']
        legendStrings.append(numberOfueNodes + ' UEs')
        ax = plotCumulative(ax, params['highestBin'], params['nBins'], cumulativeLat, xLabel, yLabel, '')
    
    ax.legend(legendStrings)
    if params['neptune']:
        uploadFigureToNeptune(params, 'cumulativeLat', fig=fig, is_picture=True)
    if save:
        fig.savefig(e2eCumLatPlotFile, bbox_inches='tight')
    plt.close(fig)

def plotCumulative(ax, highestBin, nBins, cumulative, xLabel, yLabel, legendString):
    ax.plot(np.linspace(0, highestBin, nBins), cumulative)
    ax.set_xlabel(xLabel)
    ax.set_ylabel(yLabel)
    ax.grid(True, alpha=0.75)
    ax.axis([0, highestBin, 0, 1])
    ax.legend([legendString])
    return ax

def plotPacketStatistics(params, save=True):
    uplinkLate = []
    downlinkLate = []
    lost = []
    ticks = [int(n) for n in params['numberOfueNodes']]
    destFile = getFileName(params, 'statisticsSummaryPlotFile')
    for _, result in params['results'].items():
        uplinkLate.append(result['uplinkLatePercentage'])
        downlinkLate.append(result['downlinkLatePercentage'])
        lost.append(result['droppedPercentage'])

    uplinkLate = np.array(uplinkLate)
    downlinkLate = np.array(downlinkLate)
    lost = np.array(lost)

    fig, ax = plt.subplots()
    x = np.array(ticks)
    ax.bar(x, uplinkLate + lost + downlinkLate, width=1, label='Total')
    ax.bar(x - 1/3, uplinkLate, width=1/4, label='Late + lost packets (uplink)')
    ax.bar(x, lost, width=1/4, label='Lost Packets (downlink)')
    ax.bar(x + 1/3, downlinkLate, width=1/4, label='Late packets (downlink)')
    ax.set_xticks(ticks)
    ax.set_xlabel('Number of UEs')
    ax.set_ylabel('Fraction of lost packets')
    ax.set_yscale('log')
    ax.set_ylim([0.00001, 1])
    
    ax.legend(loc='best', fancybox=True)

    title = params['generation'] + ' scenario ' + params['scenario']
    if params['generation'] == '5G': title += ' numerology ' + str(params['numerology'])
    title += ' - ' + str(len(params['seed'])) + ' simulations per bar'
    if int(params['scenario']) > 1:
        title += ' - ' + str(params['numberOfenbNodes'])
        if params['generation'] == '4G':
            title += ' eNBs'
        else:
            title += ' gNBs'
    fig.suptitle(title)
    if params['neptune']:
        uploadFigureToNeptune(params, 'statisticsSummary', fig=fig, is_picture=True)
    if save:
        fig.savefig(str(destFile))
    plt.close(fig)

def plotLatency(params):
    # latencyData = {}
    fig, ax = plt.subplots()
    prop_cycle = plt.rcParams['axes.prop_cycle']
    colors = cycle(prop_cycle.by_key()['color'])
    destFile = getFileName(params, 'latencyPlotFile')
    for ue in range(int(params['numberOfueNodes'])):
        c = next(colors)
        s = 1
        latencyData = [(x[1], x[2]) for x in params['data'] if x[0] == ue]
        lateLatencyData = [x for x in latencyData if x[1] == 18446744073709551615]
        if len(lateLatencyData) > 0:
            lateLatencyData = [(x[0]/1000000, x[1]/1000) for x in lateLatencyData]
            lateTimestamp, lateLatency = list(map(list, zip(*lateLatencyData)))
            ax.scatter(lateTimestamp, np.zeros(len(lateLatency)), color=c, marker='x', s=s)
        latencyData = [x for x in latencyData if x[1] != 18446744073709551615]
        latencyData = [(x[0]/1000000, x[1]/1000) for x in latencyData]
        timestamp, latency = list(map(list, zip(*latencyData)))
        ax.scatter(timestamp, latency, label='UE ' + str(ue), color=c, s=s)
    ax.set_xlabel('Time [s]')
    ax.set_ylabel('Latency [ms]')
    # ax.legend(loc='best', fancybox=True)
    fig.savefig(str(destFile))
    plt.close(fig)


def plotPositions(params, save=True, forced=True):
    positions = params['positions']
    marker = ''
    title = 'Positions'
    destFile = getFileName(params, 'positionsPlotFile')
    if not destFile.exists() or forced:
        radius = int(params['radius'])
        fig, ax = plt.subplots()
        for position in positions:
            x = float(position[1])
            y = float(position[2])
            if position[0] == "eNB" and position[4] == "r":
                marker = 'ro'
                circle = plt.Circle((x, y), radius, color='black', fill=False)
                ax.add_patch(circle)
            if position[0] == "UE" and position[4] == "r":
                marker = 'bo'
            if position[0] == "eNB" and position[4] == "t":
                marker = 'co'
                circle = plt.Circle((x, y), radius, color='black', fill=False)
                ax.add_patch(circle)
            if position[0] == "UE" and position[4] == "t":
                marker = 'yo'
            ax.plot(x, y, marker)

        ax.set_aspect('equal', adjustable='box')
        fig.suptitle(title)
        if params['neptune']:
            uploadFigureToNeptune(params, 'positions', fig=fig, is_picture=True)
        if save:
            fig.savefig(str(destFile), bbox_inches='tight')
        plt.close(fig)