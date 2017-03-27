import logging
from datetime import datetime
#from decimal import Decimal
from math import floor

class ConversionAndRounding:
    #def __init__(self):

    def convertToStrikeForCQGSymbol(self, barVal, tickIncrementIn, tickDisplayIn, idInstrument, is_cme_data = False):

        #instrumentIdArrayCheck = [1, 360, 2, 3, 21, 23, 25, 31, 32, 33, 34, 35, 39, 40, 42, 43, 51, 52, 53, 54, 99, 101,102, 200,210,220,360, 400,532,11,12]

        '''if (idInstrument == 39 or idInstrument == 40): # GLE or HE

            return int(barVal * tickDisplayIn)

        elif (idInstrument == 1 or idInstrument == 360):

            return int(barVal * tickDisplayIn)

        elif (idInstrument == 2 or idInstrument == 3 or idInstrument == 21 or idInstrument == 23 or idInstrument == 25 or idInstrument == 31):
        '''
        if is_cme_data:
            return int(round(barVal * tickDisplayIn))

        else:
            return int(ConversionAndRounding.convertToTickMovesDouble(self, barVal, tickIncrementIn, tickDisplayIn))


    def convertToTickMovesDouble(self, barVal, tickIncrementIn, tickDisplayIn):
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

            #print('conversion', displayVal, barVal)

            if displayVal < 0:
                fuzzyZero = -tickIncrement / 1000

            intPart = floor((displayVal + fuzzyZero) / decimalBase + fuzzyZero)

            decPart = (displayVal - intPart * decimalBase) / tickDisplay * tickIncrement

            fractPart = 0

            res = intPart + decPart + fractPart

            incrMultiple = floor(res / tickIncrement + positiveFuzzyZero) * tickIncrement

            if (res < incrMultiple + positiveFuzzyZero and res > incrMultiple - positiveFuzzyZero):
                #print('conversion', res)
                return res

            else:
                #print('conversion', (incrMultiple + tickIncrement))
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
                    return ConversionAndRounding.convertToTickMovesDoubleSpan(self, barVal, strikeIncrement, 0)

            elif idInstrument == 532: #LB lumber

                tempval = float(barVal)
                tempStrikeDisplay = float(strikeDisplay)

                return tempval / tempStrikeDisplay

            else:

                return ConversionAndRounding.convertToTickMovesDoubleSpan(self, barVal, strikeIncrement, strikeDisplay)

        except:
            print("conversion error")
            logging.exception("conversion  error")

    def convertToTickMovesDoubleSpan(self, barVal, tickIncrementIn, tickDisplayIn):
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
