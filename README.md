# BLStoElastisearchWithPerl
Perl scripts to process Bureau of Labor Statistics data and ingest into Elasticsearch

Once you have downloaded the BLS EN dataset from [https://download.bls.gov/pub/time.series/en/](https://download.bls.gov/pub/time.series/en/) and download these scripts you should have a directory structure like the one below.
```
.
├── en.area
├── en.data.1.AllData
├── en.footnote
├── en.industry
├── en.owner
├── en.series
├── en.size
├── en.state
├── en.type
├── index.html
└── src
    ├── LICENSE
    ├── output
    │   ├── data
    │   └── series
    ├── process_data_files.pl
    ├── process_series_files.pl
    ├── README.md
    ├── split_data_by_state.pl
    └── split_series_by_state.pl
```
Before running any of the scripts you should create an elasticsearch template using the .json file in this repository. This will make sure dates and values are ingested with the correct data type. My lab cluster is four nodes which is why there are 2 shards selected. This will be a need as I add more states to the index. If you have a smaller cluster or do not plan on adding any more than one or two states then you can change the number of shards to 1.

You will need host and username information for an Elasticsearch cluster that has write access to the index where you wish to ingest the data.

The scripts in the src folder should be ran in the order below:
```
split_series_by_state.pl
split_data_by_state.pl
process_series_files.pl
process_data_files.pl
```

The first two of these scripts do not require any command line options because they assume that the series and data files are one level up in the directory structure at en.series and en.data.1.AllData. The first two scripts can run at the same time as they are reading and writing to different files. The output of these two scripts will be at output/series and output/data. The files output by these scripts will have the extension .mycopy so as to not confuse them with the originals.

Once the series and data files are split process_series_files has to be ran next. This script assumes you have a local redis server running at the default port as that is what it uses to store the intermediate keys. This script requires a file name since this is setup to process one state at a time. process_series_files.pl has to complete before running process_data_files.pl. Example of process_series_files.pl below:

`perl process_series_files.pl -f output/series/en.series.Ohio.mycopy`

process_data_files.pl has a large list of required command line options so I have provided a full command below:

`perl process_data_files.pl -utempelasticuser -ptempelasticuser -h192.168.2.229:9200 -ebls-final-data-en-ohio -f output/data/en.data.Ohio.mycopy -b 2500 -i`

Watch the video here to see this in action [https://youtu.be/A6M0JW-R3lA](https://youtu.be/A6M0JW-R3lA) and this video [https://youtu.be/pU2wqrstUb8](https://youtu.be/pU2wqrstUb8) that explains how to use the BLS Ohio dashboard at [https://dangerousmetrics.org](https://dangerousmetrics.org)

