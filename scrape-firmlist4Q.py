from bs4 import BeautifulSoup
import requests
import pandas as pd
import datetime
import time
import multiprocessing
from functools import partial
requests.packages.urllib3.contrib.pyopenssl.extract_from_urllib3()

# Extracts cik codes from excel file
def codes():
    for i in range(len(df.index)):
        if df.loc[i, 'cik'] not in cik_codes:
            cik_codes.append(df.loc[i, 'cik'])
            urls.append('https://www.sec.gov/cgi-bin/browse-edgar?action=getcompany&CIK=' + df.loc[i, 'cik']
                        + '&type=10-K&dateb=&owner=exclude&count=100')

# Does all the scraping and updating the dataframe
def scrape(index, url, cik_code, df2, start):
    print(cik_code)
    response = requests.get(url[index])
    if response.status_code == 200:

        # Extracts the link for interactive data
        soup = BeautifulSoup(response.text, 'html.parser')
        table = soup.find('table', {'class': 'tableFile2'})
        if table is None:
            return df2
        a_tag = table.find_all('a')
        links = []
        for intData in a_tag:
            if 'Interactive Data' in intData.text:
                links.append('https://www.sec.gov' + intData.get('href'))

        # Extracts the dates and filing types
        table_df = pd.read_html(url[index], header=0, attrs={'class': 'tableFile2'})
        df1 = table_df[0]
        dates = []
        filingType = []
        for i in range(0, len(df1.index)):
            if 'Interactive Data' in df1.loc[i, 'Format']:
                dates.append(df1.loc[i, 'Filing Date'])
                filingType.append(df1.loc[i, 'Filings'])

        # Extracts the financial statements
        if len(links) > 0:
            found = False
            for i in range(0, len(links)):
                linkResponse = requests.get(links[i])
                if linkResponse.status_code == 200:
                    linkSoup = BeautifulSoup(linkResponse.text, 'html.parser')
                    fs_menu = linkSoup.find('ul', {'id': 'menu'})
                    if fs_menu is None:
                        break
                    fs_class = fs_menu.find_all('li', {'class': 'accordion'})
                    fs_index = 0
                    for j in range(0, len(fs_class)):
                        if '>Financial Statements<' in str(fs_class[j]) and '>Notes to Financial Statements<' not in str(fs_class[j])\
                            and '>Cover<' not in str(fs_class[j]) and '>All Reports<' not in str(fs_class[j]):
                            fs_index = j
                            break
                    fs = fs_class[fs_index].find_all('a', {'class': 'xbrlviewer'})
                    for k in range(0, len(fs)):
                        fs[k] = fs[k].text

                    # writes to the dataframe
                    for l in range(start, len(df2.index)):
                        if df2.loc[l, 'cik'] == cik_code[index]:
                            if not found:
                                start = l
                            found = True
                            d1 = datetime.datetime(int(df2.loc[l, 'datadate'][0:4]), int(df2.loc[l, 'datadate'][5:7]),
                                                   int(df2.loc[l, 'datadate'][8:10]))
                            d2 = datetime.datetime(int(df2.loc[l, 'LatestPossibleFilingDate'][0:4]),
                                                   int(df2.loc[l, 'LatestPossibleFilingDate'][5:7]),
                                                   int(df2.loc[l, 'LatestPossibleFilingDate'][8:10]))
                            d3 = datetime.datetime(int(dates[i][0:4]), int(dates[i][5:7]), int(dates[i][8:10]))
                            if d3 >= d1 and d3 <= d2:
                                df2.loc[l, 'FilingType'] = filingType[i]
                                df2.loc[l, 'FilingDate'] = dates[i]
                                df2.loc[l, 'Directory'] = links[i]
                                for m in range(0, len(fs)):
                                    df2.loc[l, 'Order' + str(m + 1)] = fs[m]
                                break
                        elif found:
                            break

    return df2

# Creates an output excel file from the dataframe
def output():
    df.to_excel('firmlist4Q_20190416.xlsx', index=False, sheet_name = '10K')

if __name__ == '__main__':

    t = time.time()
    print(datetime.datetime.now())

    # Creates a dataframe from the excel file
    df = pd.read_excel('firmlist4Q_20190416.xlsx', dtype = str)

    urls = []
    cik_codes = []

    codes()

    begin = 0
    num_pool = 100

    for num in range(0, len(urls), num_pool):

        t1 = time.time()
        print(num)
        print(begin)

        df = pd.read_excel('firmlist4Q_20190416.xlsx', dtype = str)

        # Calls the scrape method using multiprocessing
        end = num + num_pool
        if num + num_pool > len(urls):
            end = len(urls)
            num_pool = len(urls) - num
        p = multiprocessing.Pool(num_pool)
        parameters = partial(scrape, url = urls, cik_code = cik_codes, df2 = df, start = begin)
        all_dfs = p.map(parameters, range(num, end))
        try:
            p.terminate()
        except WindowsError:
            pass
        p.join()

        # Updates the dataframe
        for dfs in all_dfs:
            df[df.isnull()] = dfs
            if len(df.columns) != len(dfs.columns):
                for i in range(len(df.columns), len(dfs.columns)):
                    df[dfs.columns[i]] = dfs[dfs.columns[i]]

        output()

        if num + num_pool < len(cik_codes):
            for i in range(begin, len(df.index)):
                if df.loc[i, 'cik'] == cik_codes[num + num_pool]:
                    begin = i
                    break

        print('done in: ', time.time() - t1)
    print('all done in: ', time.time() - t)