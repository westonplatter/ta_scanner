# Technical Analaysis Scanner

## Goals

This software attempts to provide a framework that does a combination of
(1) scanning and, (2) backtesting to ask and answer such questions as,

- which instruments have responded well to the 4 hr MACD(26, 9, 12) in the
last quarter?

- which stocks are getting close to their 20/50 Moving Average Crossover, and
of those stocks, which have responded more than +/- 2.5% in the past?

- which instruments in the last 2 weeks after trended out of their opening
ranges? What is the 1 and 2 std dev band for each isntrument at 30 minute
intervals?

## Features

- [ ] Pull in live data from IB
- [ ] Apply various indicators against single or multiple instruments
    - [ ] Simple Moving Average Crossover
    - [ ] MACD Crossover
    - [ ]please open an issue for those you're interested in
s- more coming

### Digging a little deeper

Technical analysis sometimes works, and sometimes doesn't. The goal of this
codebase is to provide means/methods for measuring a universe of instruments
and determine which ones are behaving in line with various TA patterns

It's intended to work differently than a traditional backtester (eg, a
Quantopian and QuantConnect). From what I undestand about backtesting, the
goal is to provide predetermined entry and exit rules, and measure the
results for a single or multiple instruments. This software is didferent
it that it intends to experiment with the entry and exit rules and see how
those adjustments impact results.

## Structure

Core Framework lives in this repo, and your secret sauce parameter/configs,
research findings live in another one.

This repo will feel like a mono repo to many, and focuses on

- downloading market data (currently using IB API Gateway)
- exposing market data
- running multi-variate simulations
- reporting results


## Releasing
```
python setup.py sdist bdist_wheel
twine upload dist/*
```
