import math
import numpy as np

''' The generalized Black and Scholes formula

    ulprice - stock price
    strike - strike price of option
    riskfreerate - riskfreerateisk-free interest rate
    toexpiry - time to expiration in years
    v - volatility of the relative price change of the underlying stock price
    b - cost-of-carry
        b = riskfreerate        --> Black-Scholes stock option model
        b = 0        --> Black futures option model
        b = riskfreerate-q      --> Merton stock option model with continuous dividend yield q
        b = riskfreerate - riskfreerate(f) --> Garman-Kohlhagen currency option model, where riskfreerate(f) is the risk-free rate of the foreign currency

    Examples:
    a) currency option
       toexpiry = 0.5, 6 month to expiry
       ulprice = 1.56, USD/DEM exchange rate is 1.56
       strike = 1.6,  strike is 1.60
       riskfreerate = 0.06, domestic interest rate in Germany is 6% per annum
       riskfreerate(f) = 0.08, foreign risk-free interest rate in the U.S. is 8% per annum
       v = 0.12, volatility is 12% per annum
       c = 0.0291'''



'''def calculate_option_volatility(callPutFlag, ulprice, strike, toexpiry, riskfreerate, currentOptionPrice):

    tempV = 0.5;

    try:

        if toexpiry == 0:
            toexpiry = 0.0001

        i = 0

        volInc = 0.5
        maxIter = 100

        while i < maxIter:
            priceTest = blackScholes(callPutFlag, ulprice, strike, toexpiry, riskfreerate, tempV)

            if abs(currentOptionPrice - priceTest) < 0.01:
                return tempV
            elif priceTest - currentOptionPrice > 0:
                volInc /= 2
                tempV -= volInc
            else:
                volInc /= 2
                tempV += volInc

            i += 1
    except:
        print("extract_rowtype_8 error")("OptionCalcs error");


    return tempV'''


def to_expiration_years(expiration_date, current_date):
    return (expiration_date.date() - current_date.date()).total_seconds() / 31536000.0  # == (365.0 * 24 * 60 * 60)

def calculateOptionVolatilityNR(callPutFlag, ulprice, strike, toexpiry, riskfreerate, currentOptionPrice, tickSize):

    return calculateOptionVolatilityNRCalc(callPutFlag, ulprice, strike, toexpiry, riskfreerate, currentOptionPrice,
                                       tickSize)

def calculateOptionVolatilityNRxxx(callPutFlag, ulprice, strike, toexpiry, riskfreerate, currentOptionPrice):

    return calculateOptionVolatilityNRCalc(callPutFlag, ulprice, strike, toexpiry, riskfreerate, currentOptionPrice,
                                        0.0001)

def calculateOptionVolatilityNRCalc(callPutFlag, ulprice, strike, toexpiry, riskfreerate, currentOptionPrice, epsilon):

    vi = 0
    ci = 0
    vegai = 0
    prevVi = 0
    b = 0

    try:

        if (toexpiry == 0):
            toexpiry = 0.0001

        vi = math.sqrt(abs(math.log(ulprice / strike) + riskfreerate * toexpiry) * 2 / toexpiry)

        ci = blackScholes(callPutFlag, ulprice, strike, toexpiry, riskfreerate, vi)
        vegai = gVega(ulprice, strike, toexpiry, riskfreerate, b, vi)

        maxIter = 100
        i = 0

        prevVi = vi

        priceDifference = abs(currentOptionPrice - ci)
        smallestPriceDifference = priceDifference

        while (priceDifference > (epsilon / 10) and i < maxIter):

            if (priceDifference < smallestPriceDifference and vi <= prevVi
                and vi > 0 and ci > 0):

                prevVi = vi
                smallestPriceDifference = priceDifference


            vi = math.abs(vi - (ci - currentOptionPrice) / vegai)

            ci = blackScholes(callPutFlag, ulprice, strike, toexpiry, riskfreerate, vi)

            priceDifference = math.abs(currentOptionPrice - ci)

            if (vi <= 0 or math.isinf(vi) or math.isnan(vi)):
                vi = prevVi
                break #break the while loop

            vegai = gVega(ulprice, strike, toexpiry, riskfreerate, b, vi)

            i += 1

            if i == maxIter:
                vi = prevVi
        return vi

    except:
        if math.isinf(vi) or vi < 0 or math.isnan(vi):
            return 0
        else:
            return vi


def blackScholes(callputflag, ulprice, strike, toexpiry, riskfreerate, iv):
    try:
        if toexpiry <= 0:
            # Calculate payoff at expiration
            if callputflag == 'C' or callputflag == 'c':
                return max(0.0, ulprice - strike)
            else:
                return max(0.0, strike - ulprice)

        d1 = (math.log(ulprice / strike) + (riskfreerate + iv * iv / 2) * toexpiry) / (iv * math.sqrt(toexpiry))
        d2 = d1 - iv * math.sqrt(toexpiry)

        if callputflag == 'C' or callputflag == 'c':
            bsPrice = ulprice * cnd(d1) - strike * math.exp(-riskfreerate * toexpiry) * cnd(d2)
        else:
            bsPrice = strike * math.exp(-riskfreerate * toexpiry) * cnd(-d2) - ulprice * cnd(-d1)
        return bsPrice
    except:
        return 0.0


def cnd(d):
    A1 = 0.31938153
    A2 = -0.356563782
    A3 = 1.781477937
    A4 = -1.821255978
    A5 = 1.330274429
    RSQRT2PI = 0.39894228040143267793994605993438
    K = 1.0 / (1.0 + 0.2316419 * np.abs(d))
    ret_val = (RSQRT2PI * np.exp(-0.5 * d * d) *
               (K * (A1 + K * (A2 + K * (A3 + K * (A4 + K * A5))))))
    if d > 0:
        return 1.0 - ret_val
    else:
        return ret_val


def nd(X):
    try:
        return math.exp(-X * X / 2) / math.sqrt(2 * math.pi)
    except:
        return 0


def gVega(ulprice, strike, toexpiry, riskfreerate, b, iv):
    try:
        if toexpiry == 0:
            toexpiry = 0.0001

        d1 = (math.log(ulprice / strike) + (b + math.pow(iv, 2) / 2) * toexpiry) / (iv * math.sqrt(toexpiry))

        return ulprice * math.exp((b - riskfreerate) * toexpiry) * nd * math.sqrt(toexpiry)

    except:
        return 0

