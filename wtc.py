from bs4 import BeautifulSoup as BS
import subprocess
import urllib3
import time
import logging
 
logging.basicConfig(filename="/home/ubuntu/wtc-cron.log", level=logging.DEBUG)
errors = open('/home/ubuntu/error', 'wb') # add filemode="w" to overwrite

url_base = 'https://wtc.ycht.io/'
outFile = '__ledger.csv'

def last_block():
        """ Find the last block saved from the last session """
        lines = open (outFile, 'r').readlines(1000)
        lastblock = str(lines[1][0:6]).strip(',')
        return lastblock

def get_page(url):
        """ Parse the web page table with urllib3, BeautifulSoup and requests modules """
        http = urllib3.PoolManager()
        response = http.request('GET', url)
        soup = BS(response.data, 'lxml')
        htmlTable = soup.find_all('table')[0] # Grab the first table
        return htmlTable

def chunks(l, n):
        """ Yield successive n-sized chunks from l """
        for i in range(0, len(l), n):
                yield l[i:i + n]

def collect_data():
        """ Process the parsed table from the webpage and export to a CSV """
        telMessage = 'This file is from the block number: %s' % lastBlock
        for page in range(1,3999):
                time.sleep(2)
                dataList = []
                url = url_base + '?page=' + str(page)
                print ('Processing page:', page)
                table = get_page(url)
                for row in table.find_all('tr'):
                        cols = row.find_all('td')
                        for col in cols:
                                dataList.append(col.text)
                for miniList in list(chunks(dataList, 5)):
                        if (miniList[0] == 'Block'): 
                                continue
                        """ Run the loop until reaching to last block number from the previous session """
                        if (str(miniList[0]).strip() <= str(lastBlock).strip()):
                                print (miniList[0].strip(), lastBlock)
                                ledgerFile.write('\n')
                                ledgerFile.close()
                                time.sleep(15)
                                """ Send the file as a telegram message """
                                subprocess.run(["/usr/local/bin/telegram-send", telMessage], stdout=errors, stderr=subprocess.STDOUT)
                                subprocess.run(["/usr/local/bin/telegram-send", "--file", outFile], stdout=errors, stderr=subprocess.STDOUT)
                                exit()
                        else:
                                """ Write line-by-line to CSV file """
                                print (miniList[0].strip(), lastBlock.strip())
                                ledgerFile.write(miniList[0].strip('\n').strip()) 
                                ledgerFile.write(miniList[1].replace('\n', ',').strip())
                                ledgerFile.write(miniList[2].replace('\n', ',').strip())
                                ledgerFile.write(",")
                                ledgerFile.write(miniList[3])
                                ledgerFile.write(",")
                                ledgerFile.write(miniList[4])
                                ledgerFile.write('\n')

""" Find the last block from the previous session """
lastBlock = last_block()
print ('lastBlock is:', lastBlock)

""" Open the file, flush it and write the headers """
ledgerFile = open(outFile, 'w')
ledgerFile.flush()
ledgerFile.write('Block,Miner,ExtraData,GasUsed,Date_UTC\n')

collect_data()
