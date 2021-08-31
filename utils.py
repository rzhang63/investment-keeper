import scipy.optimize
import requests
import execjs
import difflib
import json


def xnpv(valuesPerDate, rate):
    DAYS_PER_YEAR = 365.0
    assert isinstance(valuesPerDate, list)
    if rate == -1.0:
        return float('inf')

    #t0 = min(list(map(lambda x: x[0], valuesPerDate)))
    t0 = min(valuesPerDate,key=lambda x:x[0])[0]

    if rate <= -1.0:
        return sum([-abs(vi) / (-1.0 - rate) ** ((ti - t0).days / DAYS_PER_YEAR) for ti, vi in valuesPerDate])

    return sum([vi / (1.0 + rate) ** ((ti - t0).days / DAYS_PER_YEAR) for ti, vi in valuesPerDate])


def xirr(valuesPerDate):
    assert isinstance(valuesPerDate, list)
    if not valuesPerDate:
        return None
    list(map(lambda x: x[1], valuesPerDate))
    
    if all(x[1] >= 0 for  x in valuesPerDate):
        return float("inf")
    if all(x[1] <= 0 for  x in valuesPerDate):
        return -float("inf")

    result = None
    try:
        result = scipy.optimize.newton(lambda r: xnpv(valuesPerDate, r), 0)
    except (RuntimeError, OverflowError):  # Failed to converge?
        result = scipy.optimize.brentq(lambda r: xnpv(valuesPerDate, r), -0.999999999999999, 1e20, maxiter=10 ** 6)

    if not isinstance(result, complex):
        return result
    else:
        return None


def downloadAllFundCode():
    url = 'http://fund.eastmoney.com/js/fundcode_search.js'
    content = requests.get(url)
    jsContent = execjs.compile(content.text)
    rawData = jsContent.eval('r')
    code2name = {}
    name2code = {}
    for x in rawData:
        code2name[x[0]] = x[2]
        name2code[x[2]] = x[0]
    
    with open("./name2code.json", "w") as outfile:
        json.dump(name2code, outfile)
    with open("./code2name.json", "w") as outfile:
        json.dump(code2name, outfile)

def getAllFundCode():
    with open('./name2code.json') as json_file:
        name2code = json.load(json_file)
    with open('./code2name.json') as json_file:
        code2name = json.load(json_file)
    return name2code,code2name

def get_closest_fund_code(s,name2code):
    matched_name = max([(k,difflib.SequenceMatcher(a=k, b=s).ratio()) for k in name2code.keys()],key=lambda x:x[1])[0]
    return name2code[matched_name]+matched_name
        
