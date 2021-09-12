import requests
import gzip
import shutil
import csv
import re
import matplotlib.pyplot as plt
plt.rcdefaults()
import pandas as pd
import mysql.connector


def download_file(url, name):
    r = requests.get(url, allow_redirects=True)
    open(name, 'wb').write(r.content)


def unzip_file(name):
    with gzip.open(name+'.tsv.gz', 'rb') as f_in:
        with open(name+'.tsv', 'wb') as f_out:
            shutil.copyfileobj(f_in, f_out)


def download_data():
    nights_url = 'https://ec.europa.eu/eurostat/estat-navtree-portlet-prod/BulkDownloadListing?file=data/tour_occ_nim.tsv.gz'
    nights_nr_url = 'https://ec.europa.eu/eurostat/estat-navtree-portlet-prod/BulkDownloadListing?file=data/tour_occ_ninrmw.tsv.gz'
    arrivals_url = 'https://ec.europa.eu/eurostat/estat-navtree-portlet-prod/BulkDownloadListing?file=data/tour_occ_arm.tsv.gz'
    arrivals_nr_url = 'https://ec.europa.eu/eurostat/estat-navtree-portlet-prod/BulkDownloadListing?file=data/tour_occ_arnrmw.tsv.gz'
    download_file(nights_url, 'nights.tsv.gz')
    download_file(nights_nr_url, 'nights-nr.tsv.gz')
    download_file(arrivals_url, 'arrivals.tsv.gz')
    download_file(arrivals_nr_url, 'arrivals-nr.tsv.gz')


def unzip_files():
    unzip_file('nights')
    unzip_file('nights-nr')
    unzip_file('arrivals')
    unzip_file('arrivals-nr')


def read_file(name):
    results = []
    with open(name+'.tsv') as fd:
        rd = csv.reader(fd, delimiter="\t", quotechar='"')
        for row in rd:
            results.append(row)
    return results


# filters data rows, keeps only the 2 most general rows for the countries we care about
def filter_rows(data):
    filtered_data = []
    header = data[0]

    # see which headers need to be tested
    has_partner = False
    has_c_res = False
    if 'partner' in header[0]:
        has_partner = True
    if 'c_resid' in header[0]:
        has_c_res = True

    # add header to output
    filtered_data.append(header)
    data.pop(0)

    # add to output only the necessary data
    for row in data:
        h = row[0]
        valid = True
        # unit should be NR and nace_r2 should be I551-I553
        if 'NR' not in h or 'I551-I553' not in h:
            valid = False
        # if partner exists, it should be WORLD
        if has_partner and 'WORLD' not in h:
            valid = False
        # if c_res exists, it should be TOTAL
        if has_c_res and 'TOTAL' not in h:
            valid = False
        # geo should be EL or SE
        if 'EL' not in h and 'SE' not in h:
            valid = False

        if valid:
            filtered_data.append(row)

    return filtered_data


# filters data columns, keeps the right months (and the row headers)
def filter_columns(data):
    filtered_data = []
    header = data[0]

    start = header.index('2011M01 ')
    end = header.index('2007M01 ')+1

    for row in data:
        filtered_data.append([row[0]]+row[start:end])

    return filtered_data


# combines remove flags + filter columns + rows
def filter_data(data):
    return remove_flags(filter_columns(filter_rows(data)))


# some data entries are like '4560.0 c' where c is a flag. remove flags and other non numeric symbols
def remove_flags(data):
    output = []
    output.append(data[0])

    for i in range(1, 3):
        new_row = []
        for ind, r in enumerate(data[i]):
            if ind != 0:
                new_row.append(re.sub("[^0-9]", "", r))
            else:
                new_row.append(r)
        output.append(new_row)

    return output


# create a pandas dataframe and export to CSV format
def export_data_csv(data, name):
    df = pd.DataFrame(data[1:], columns=data[0])
    df.to_csv(name+'.csv', index=False, encoding='utf-8')


# send the data to an external MySQL database
def export_data_sql(data, table_name):
    db = mysql.connector.connect(
        host="127.0.0.1",
        user="admin",
        database="python-project"
    )
    cursor = db.cursor()
    sql = 'INSERT INTO ' + table_name + ' VALUES (%s, %s, %s)'

    for row in data[1:]:
        for i, r in enumerate(row[1:]):
            val = (row[0], data[0][i+1], r)
            cursor.execute(sql, val)

    db.commit()


# plots the data using matplotlib
def plot_data(data, xlabel, ylabel, title):
    x_pos = range(1, len(data))

    plt.bar(x_pos, list(map(int, data[1:])), align='center', alpha=0.7)
    plt.xticks()
    plt.ylabel(ylabel)
    plt.xlabel(xlabel)
    plt.title(title)

    plt.show()


if __name__ == '__main__':
    # download data
    download_data()
    unzip_files()
    # put data in memory
    nights = read_file('nights')
    nights_nr = read_file('nights-nr')
    arrivals = read_file('arrivals')
    arrivals_nr = read_file('arrivals-nr')
    # filter data
    nights = filter_data(nights)
    nights_nr = filter_data(nights_nr)
    arrivals = filter_data(arrivals)
    arrivals_nr = filter_data(arrivals_nr)

    # plot data
    plot_data(nights[1], 'Month', 'Nights spent at tourist accommodation establishments', 'Greece')
    plot_data(nights[2], 'Month', 'Nights spent at tourist accommodation establishments', 'Sweden')
    plot_data(nights_nr[1], 'Month', 'Nights spent by non-residents at tourist accommodation establishments', 'Greece')
    plot_data(nights_nr[2], 'Month', 'Nights spent by non-residents at tourist accommodation establishments', 'Sweden')
    plot_data(arrivals[1], 'Month', 'Arrivals at tourist accommodation establishments', 'Greece')
    plot_data(arrivals[2], 'Month', 'Arrivals at tourist accommodation establishments', 'Sweden')
    plot_data(arrivals_nr[1], 'Month', 'Arrivals of non-residents at tourist accommodation establishments', 'Greece')
    plot_data(arrivals_nr[2], 'Month', 'Arrivals of non-residents at tourist accommodation establishments', 'Sweden')

    # export data to CSV files
    export_data_csv(nights, 'nights')
    export_data_csv(nights_nr, 'nights_nr')
    export_data_csv(arrivals, 'arrivals')
    export_data_csv(arrivals_nr, 'arrivals_nr')

    # add data to database
    export_data_sql(nights, 'nights')
    export_data_sql(nights_nr, 'nights_nr')
    export_data_sql(arrivals, 'arrivals')
    export_data_sql(arrivals_nr, 'arrivals_nr')
