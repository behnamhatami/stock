# Tehran Stock Market Crawler and analyzer

[![made-with-python](https://img.shields.io/badge/Made%20with-Python-1f425f.svg)](https://www.python.org/)

A django web app that helps you to crawl `TSETMC` share lists, share groups, historical data and live data of the day

## Features

- Download list of shares
- Download list of share groups
- Download history of share prices
- Download last day data of the shares including transaction board, prices and transaction datas
- Store historical data in database
- Efficient indexing and analyzing using `PANDAS` dataframe
- Normalizing Data of share prices due to events in share data
- Nice admin page for searching and editing data by using `Django` web framework
- Compatible with linux cron job system for crawling new historical data every day
- Compatible with `Django`
- Compatible with `PANDAS`
- Compatible with `sqlite` or every other production database supported by `Django`

## 0 - Install
```bash
git clone https://github.com/behnamhatami/stock
cd stock
sudo pip3 install -r requirements.txt 
```

## 1- Initialization
For first use you need initialize the database
```
python3 manage.py migrate
```

then you should crawl the data of the stock lists
```
python3 manage.py update_share_list
```

also you can deep crawl tse site for shares (the closed shares in last active day of bazaar) by searching
```
python3 manage.py update_share_list_by_search
```

## 2- Downloading Historical Data
For downloading historical data you can use this command
```
python3 manage.py update_share_history
```
if tooks couple of minute depends on your internet quality and `TSETMC` site speed to download all historical data of listed shares. It will give stat of the process during crawling `TSETMC` site.

## 3- Access Data

To access data you can use `Django` model `Share` object, which helps you to find your share, accessing its history and analyzing it. You can also find your data in `Django` admin.
