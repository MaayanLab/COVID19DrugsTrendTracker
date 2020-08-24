# COVID-19 Drug Trend Tracker on Twitter
This repository provides a daily summary of tweets mentioning drugs related to COVID-19. 

## Twitter REST API
Tweets are collected using Twitter REST API by searching for drug names.
Only tweets containing known drug names and different lingustic variations of the keywords "COVID-19" or "SARS" are considered.

## Twitter Stream API
Please see ```/code/stream``` folder

Tweets identified by Twitter's COVID-19 stream tweet annotations.

These tweets are captured in real-time through a streaming connection.

The data contains full conversation about this topic, rather than a sample (using the basic API).

To produce results, we ingest, process, and analyze streaming data at a scale of tens of millions of tweets per day.

