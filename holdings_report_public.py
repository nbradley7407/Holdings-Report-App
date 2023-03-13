from requests_pkcs12 import post
import json
import requests
import os
import pandas as pd


class HoldingsReport:

    def __init__(self, cert, pkcs12_password, csv_path, api_url, excel_path, auth):
        # cert = file path to p12 file, csv_path used to pull account names, excel_path used to write data to excel,
        # auth is encrypted consumer user and secret
        self.cert = cert
        self.pkcs12_password = pkcs12_password
        self.csv_path = csv_path
        self.api_url = api_url
        self.excel_path = excel_path
        self.auth = auth


    def _get_token(self):
        # first get a token to access Holdings API
        self.url = "https://apigatewayb2b.qa.bnymellon.com/token"
        self.auth_payload = 'grant_type=client_cert'
        self.auth_headers = {
            'Authorization': f'Basic {self.auth}',
            'Content-Type': 'application/x-www-form-urlencoded'
        }

        response = post(self.url,
                        pkcs12_filename=self.cert,
                        pkcs12_password=self.pkcs12_password,
                        headers=self.auth_headers,
                        data=self.auth_payload)
        json_object = response.json()
        self.my_token = json_object['access_token']
        print(f'Token generated: {self.my_token}')

    def _get_holdings(self):
        # open csv with account numbers and iterate over each one to make a call to holdings api using token
        with open(f'{self.csv_path}', 'r') as f:
            self.holdings_data = []                          # list to store holdings data
            self.num_accounts = sum(1 for _ in f)            # sum number of accounts
            self.counter = 1                                 # track which account the script is running
            f.seek(0)
            for i in f:
                print(f'Scanning {i} [{self.counter}/{self.num_accounts}]')
                self.counter += 1
                i = i.replace('\n', '')
                scroll_key = ''
                for _ in range(10):
                    # get current account number's info, scroll if needed to get all info
                    self.endpoint = self.api_url
                    self.data = json.dumps({"accountIdentifier": f'{i}', "scrollKey": f"{scroll_key}"})
                    self.headers = {"Authorization": f"Bearer {self.my_token}"}
                    response = requests.post(self.endpoint, data=self.data, headers=self.headers, json=True)
                    result = response.json()

                    # grab data for current account
                    for item in result["holdings"]:
                        account_data = []                      # empty list to store individual account holdings data

                        account_data.append(i)                        # add account number to sub list
                        security_details = item['securityDetails']
                        ticker_symbol = security_details.get("tickerSymbol", 'n/a')
                        account_data.append(ticker_symbol)            # add ticker to sub list

                        holding_details2 = item["securityDetails"]["securityDescription"]
                        account_data.append(holding_details2)         # add security name to sub list

                        holding_details = item["holdingsDetails"]["marketPrice"]
                        account_data.append(holding_details)          # add market price to sub list

                        self.holdings_data.append(account_data)       # add all data to main list

                    # check for a scrollkey, otherwise move on to next account number
                    try:
                        result["scrollKey"]
                    except KeyError:
                        break
                    scroll_key = result["scrollKey"]
                    print(scroll_key)

        # move holdings data to a dataframe
        df = pd.DataFrame(self.holdings_data, columns=['Account Number', 'Ticker', 'Name', 'Price'])

        # create Excel file if there isn't one, otherwise append existing
        if not os.path.exists(f'{self.excel_path}'):
            with pd.ExcelWriter(f'{self.excel_path}', mode='w') as writer:
                df.to_excel(writer, sheet_name='dump', index=False)
        else:
            with pd.ExcelWriter(f'{self.excel_path}', mode='a', if_sheet_exists='replace') as writer:
                df.to_excel(writer, sheet_name='dump', index=False)

    def run(self):
        self._get_token()
        self._get_holdings()
