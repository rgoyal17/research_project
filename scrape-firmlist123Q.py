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

def findTable(allTags):
    tableExists = False
    statementsTable = ''
    for tags in allTags:
        if 'item 8. financial statements and supplementary data.' == ' '.join(tags.text.split()).casefold() or\
            'item 8. financial statements and supplementary data' == ' '.join(tags.text.split()).casefold() or\
            'item 8. financial statements and supplementary financial information' == ' '.join(tags.text.split()).casefold() and\
            tableExists == False:
            theTable = tags.find_next('table')
            all_tr = theTable.find_all('tr') if theTable else ''
            count = 0
            for tr in all_tr:
                if 'consolidated' in tr.text.casefold() or 'statement' in tr.text.casefold() or 'balance' in tr.text.casefold():
                    count += 1
            if count > 1:
                tableExists = True
                statementsTable = theTable
    return tableExists, statementsTable

def scrape(index, url, cik_code, df2, start):
    response = requests.get(url[index])
    if response.status_code == 200:
        # Extracts the link for interactive data
        soup = BeautifulSoup(response.text, 'html.parser')
        table = soup.find('table', {'class': 'tableFile2'})
        if table is None:
            return
        tr_tag = table.find_all('tr')
        links = []
        for documentLink in tr_tag:
            if 'Interactive Data' not in documentLink.text and 'Documents' in documentLink.text:
                a_tags = documentLink.find_all('a')
                for a_tag in a_tags:
                    if 'Documents' in a_tag.text:
                        links.append('https://www.sec.gov' + a_tag.get('href'))

        if len(links) > 0:
            # Extracts the dates and filing types
            table_df = pd.read_html(url[index], header=0, attrs={'class': 'tableFile2'})
            df1 = table_df[0]
            dates = []
            filingType = []
            for j in range(0, len(df1.index)):
                if 'Interactive Data' not in df1.loc[j, 'Format']:
                    dates.append(df1.loc[j, 'Filing Date'])
                    filingType.append(df1.loc[j, 'Filings'])

            found = False
            for i in range(len(links)):
                linkResponse = requests.get(links[i])
                if linkResponse.status_code == 200:
                    linkSoup = BeautifulSoup(linkResponse.text, 'html.parser')
                    linkTable = linkSoup.find('table', {'class':'tableFile'})
                    documentDataLink = linkTable.find('a').get('href')
                    if '.htm' in documentDataLink:
                        documentDataLink = 'https://www.sec.gov' + documentDataLink
                        htmResponse = requests.get(documentDataLink)
                        if htmResponse.status_code == 200:
                            htmSoup = BeautifulSoup(htmResponse.text, 'html.parser')

                            allTables = htmSoup.find_all('table')
                            tableExists, statementsTable = findTable(allTables)
                            if not tableExists:
                                allParas = htmSoup.find_all('p')
                                tableExists, statementsTable = findTable(allParas)
                            if not tableExists:
                                allFonts = htmSoup.find_all('font')
                                tableExists, statementsTable = findTable(allFonts)
                            if not tableExists:
                                allBolds = htmSoup.find_all('b')
                                tableExists, statementsTable = findTable(allBolds)
                            if not tableExists:
                                allHeadings = htmSoup.find_all('h2')
                                tableExists, statementsTable = findTable(allHeadings)

                            if tableExists:
                                rows = statementsTable.find_all('td')
                                statements = []
                                for k in range(0, len(rows)):
                                    fs = rows[k].text
                                    if fs.strip():
                                        if ('consolidated' in fs.casefold() or 'statement' in fs.casefold() or 'balance' in fs.casefold()) and\
                                                ('financial' not in fs.casefold() and 'report' not in fs.casefold() and 'index' not in fs.casefold()):
                                            if ',' in fs:
                                                fs = fs[0:fs.index(',')]
                                            if '\n' in fs:
                                                fs = fs.replace('\n','')
                                            fs = " ".join(fs.split())
                                            statements.append(fs)

                                # writes to the dataframe
                                if len(statements) > 0:
                                    for l in range(start, len(df2.index)):
                                        if df2.loc[l, 'cik'] == cik_code[index]:
                                            if not found:
                                                start = l
                                            found = True
                                            d1 = datetime.datetime(int(df2.loc[l, 'datadate'][0:4]),
                                                                   int(df2.loc[l, 'datadate'][5:7]),
                                                                   int(df2.loc[l, 'datadate'][8:10]))
                                            d2 = datetime.datetime(int(df2.loc[l, 'LatestPossibleFilingDate'][0:4]),
                                                                   int(df2.loc[l, 'LatestPossibleFilingDate'][5:7]),
                                                                   int(df2.loc[l, 'LatestPossibleFilingDate'][8:10]))
                                            d3 = datetime.datetime(int(dates[i][0:4]), int(dates[i][5:7]), int(dates[i][8:10]))
                                            if d3 >= d1 and d3 <= d2:
                                                df2.loc[l, 'FilingType'] = filingType[i]
                                                df2.loc[l, 'FilingDate'] = dates[i]
                                                df2.loc[l, 'Directory'] = documentDataLink
                                                for m in range(0, len(statements)):
                                                    df2.loc[l, 'Order' + str(m + 1)] = statements[m]
                                                break
                                        elif found:
                                            break
    print(str(index) + ' done')
    return df2

# Creates an output excel file from the dataframe
def output():
    df.to_excel('firmlist4Q_20190416.xlsx', index=False, sheet_name = '10K')

if __name__ == '__main__':

    t = time.time()
    print(datetime.datetime.now())

    # Creates a dataframe from the excel file
    df = pd.read_excel('firmlist4Q_20190416.xlsx', dtype=str)

    urls = []
    cik_codes = []

    codes()

    begin = 2718
    num_codes = 200
    num_pool = 8

    for num in range(350, len(urls), num_codes):

        t1 = time.time()
        print(num)
        print(begin)

        df = pd.read_excel('firmlist4Q_20190416.xlsx', dtype=str)

        # Calls the scrape method using multiprocessing
        end = num + num_codes
        if num + num_codes > len(urls):
            end = len(urls)
            num_codes = len(urls) - num

        p = multiprocessing.Pool(num_pool)
        parameters = partial(scrape, url=urls, cik_code=cik_codes, df2=df, start=begin)
        all_dfs = p.map(parameters, range(num, end))
        try:
            p.terminate()
        except WindowsError:
            print('There was an ERROR')
            pass
        p.join()

        # Updates the dataframe
        for dfs in all_dfs:
            df[df.isnull()] = dfs
            if len(df.columns) != len(dfs.columns):
                for i in range(len(df.columns), len(dfs.columns)):
                    df[dfs.columns[i]] = dfs[dfs.columns[i]]

        output()

        if num + num_codes < len(cik_codes):
            for i in range(begin, len(df.index)):
                if df.loc[i, 'cik'] == cik_codes[num + num_codes]:
                    begin = i
                    break

        print('done in: ', time.time() - t1)
    print('all done in: ', time.time() - t)