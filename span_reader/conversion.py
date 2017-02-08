import logging
from datetime import datetime
#from decimal import Decimal
from math import floor

class ConversionAndRounding:
    #def __init__(self):


    def convertToTickMovesDouble(self, barVal = 0, tickIncrementIn = 0, tickDisplayIn = 0):
        '''
        Converts double value to nearest tick
        :param barVal:
        :param tickIncrement:
        :param tickDisplay:
        :return:
        '''

        tickIncrement = float(tickIncrementIn)
        tickDisplay = float(tickDisplayIn)

        if tickDisplay == 0:
            return barVal
        try:
            fuzzyZero = tickIncrement / 1000
            positiveFuzzyZero = tickIncrement / 1000

            nTicksInUnit = floor(1 / tickIncrement + positiveFuzzyZero)

            maxFractUnits = (nTicksInUnit - 1) * tickDisplay

            decimalBase = 1
            while (maxFractUnits + positiveFuzzyZero) / decimalBase >= 1:
                decimalBase *= 10

            displayVal = float(barVal)

            print('conversion', displayVal, barVal)

            if displayVal < 0:
                fuzzyZero = -tickIncrement / 1000

            intPart = floor((displayVal + fuzzyZero) / decimalBase + fuzzyZero)

            decPart = (displayVal - intPart * decimalBase) / tickDisplay * tickIncrement

            fractPart = 0

            res = intPart + decPart + fractPart

            incrMultiple = floor(res / tickIncrement + positiveFuzzyZero) * tickIncrement

            if (res < incrMultiple + positiveFuzzyZero and res > incrMultiple - positiveFuzzyZero):
                print('conversion', res)
                return res

            else:
                print('conversion', (incrMultiple + tickIncrement))
                return incrMultiple + tickIncrement;


        except:
            print("conversion error")
            logging.exception("conversion  error")


    def convertToStrikeFromSpanData(self, barVal, strikeIncrement, strikeDisplay, \
                                    idInstrument, currentFileDate):
        '''
        Converts strike to nearest strike increment
        :param barVal:
        :param strikeIncrement:
        :param strikeDisplay:
        :param idInstrument:
        :param currentFileDate:
        :return:
        '''

        try:
            if idInstrument == 1 or idInstrument == 360: #USA=1 ULA=360

                if currentFileDate >= datetime(2016, 3, 7):

                    tempval = float(barVal)
                    tempStrikeDisplay = float(strikeDisplay)

                    return tempval / tempStrikeDisplay

                else:
                    return ConversionAndRounding.convertToTickMovesDoubleSpan(barVal, strikeIncrement, 0)

            elif idInstrument == 532: #LB lumber

                tempval = float(barVal)
                tempStrikeDisplay = float(strikeDisplay)

                return tempval / tempStrikeDisplay

            else:

                return ConversionAndRounding.convertToTickMovesDoubleSpan(barVal, strikeIncrement, strikeDisplay)

        except:
            print("conversion error")
            logging.exception("conversion  error")

    def convertToTickMovesDoubleSpan(self, barVal = 0, tickIncrementIn = 0, tickDisplayIn = 0):
        '''
        Converts double value to nearest tick
        :param barVal:
        :param tickIncrement:
        :param tickDisplay:
        :return:
        '''

        tickIncrement = float(tickIncrementIn)
        tickDisplay = float(tickDisplayIn)

        if tickDisplay == 0:
            return float(barVal)
        try:
            fuzzyZero = tickIncrement / 1000
            positiveFuzzyZero = tickIncrement / 1000

            nTicksInUnit = floor(1 / tickIncrement + positiveFuzzyZero)

            maxFractUnits = 0
            if nTicksInUnit == 1 and tickDisplay > 0:
                maxFractUnits = nTicksInUnit * tickDisplay
            else:
                maxFractUnits = (nTicksInUnit - 1) * tickDisplay

            decimalBase = 1
            while (maxFractUnits + positiveFuzzyZero) / decimalBase >= 1:
                decimalBase *= 10

            displayVal = float(barVal)

            if displayVal < 0:
                fuzzyZero = -tickIncrement / 1000

            intPart = floor((displayVal + fuzzyZero) / decimalBase + fuzzyZero)

            decPart = (displayVal - intPart * decimalBase) / tickDisplay * tickIncrement

            incrementFixTest = (displayVal - intPart * decimalBase) % (tickIncrement * decimalBase)

            incrementFix = 0

            if incrementFixTest != 0:
                incrementFix = ((tickIncrement * decimalBase) - incrementFixTest) / decimalBase

            return intPart + decPart + incrementFix

        except:
            print("conversion error")
            logging.exception("conversion  error")
