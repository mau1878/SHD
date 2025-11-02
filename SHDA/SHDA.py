"""
When this code was created only God and I knew how it works, now only God knows.

GoodLuck and may the force be with you.
"""
from os import path
import re
import json
import datetime
import requests
import numpy as np
import pandas as pd

# Lazy import for pyquery to avoid setup-time errors
pq = None

from .portfolio import Portfolio
from .common import brokers, BrokerNotSupportedException,convert_to_numeric_columns, SessionException



class SHDA:
    __settlements_int_map = {
        '1': 'spot',
        '2': '24hs',
        '3': '48hs'}

    __personal_portfolio_index = ['symbol', 'settlement']
    __personal_portfolio_columns = ['symbol', 'settlement', 'bid_size', 'bid', 'ask', 'ask_size', 'last', 'change', 'open', 'high', 'low', 'previous_close', 'turnover', 'volume', 'operations', 'datetime', 'expiration', 'strike', 'kind', 'underlying_asset', 'close']
    __empty_personal_portfolio = pd.DataFrame(columns=__personal_portfolio_columns).set_index(__personal_portfolio_index)

    __repos_index = ['symbol', 'settlement']
    __repos_columns = ['symbol', 'days', 'settlement', 'bid_amount', 'bid_rate', 'ask_rate', 'ask_amount', 'last', 'change', 'open', 'high', 'low', 'previous_close', 'turnover', 'volume', 'operations', 'datetime', 'close']
    __empty_repos = pd.DataFrame(columns=__repos_columns).set_index(__repos_index)


    __call_put_map = {
            0: '',
            1: 'CALL',
            2: 'PUT'}
    __boards = {
            0:"",
            'accionesLideres':'bluechips',
            'panelGeneral':'general_board',
            'cedears': 'cedears',
            'rentaFija':'government_bonds',
            'letes':'short_term_government_bonds',
            'obligaciones':'corporate_bonds'}

    __settlements_map = {'':0,'spot': 1,'24hs': 2,'48hs': 3}
    __securities_columns = ['symbol', 'settlement', 'bid_size', 'bid', 'ask', 'ask_size', 'last', 'change', 'open', 'high', 'low', 'previous_close', 'turnover', 'volume', 'operations', 'datetime', 'group']
    __filter_columns = ['Symbol', 'Term', 'BuyQuantity', 'BuyPrice', 'SellPrice', 'SellQuantity', 'LastPrice', 'VariationRate', 'StartPrice', 'MaxPrice', 'MinPrice', 'PreviousClose', 'TotalAmountTraded', 'TotalQuantityTraded', 'Trades', 'TradeDate', 'Panel']
    __numeric_columns = ['last', 'open', 'high', 'low', 'volume', 'turnover', 'operations', 'change', 'bid_size', 'bid', 'ask_size', 'ask', 'previous_close']
    __numeric_columns_sp = ['last', 'high', 'low','change']
    __filter_columns_sp = ['Symbol', 'LastPrice', 'VariationRate', 'MaxPrice', 'MinPrice', 'Panel']
    __sp_columns=['symbol','last','change','high','low','group']
    
    def __init__(self,broker,dni,user,password):
        global pq
        if pq is None:
            try:
                from pyquery import PyQuery as pq
            except ImportError:
                raise ImportError("pyquery is required but not installed. Run 'pip install pyquery'.")

        self.__s = requests.session()
        self.__host = self.__get_broker_data(broker)['page']
        self.__is_user_logged_in = False

        headers = {
            "Host" : f"{self.__host}",
            "User-Agent" : "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:91.0) Gecko/20100101 Firefox/91.0",
            "Accept" : "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
            "Accept-Language" : "en-US,en;q=0.5",
            "Accept-Encoding" : "gzip, deflate",
            "DNT" : "1",    
            "Connection" : "keep-alive",    
            "Upgrade-Insecure-Requests" : "1",
            "Sec-Fetch-Dest" : "document",
            "Sec-Fetch-Mode" : "navigate",
            "Sec-Fetch-Site" : "none",
            "Sec-Fetch-User" : "?1"   
        }


        response = self.__s.get(url = f"https://{self.__host}", headers=headers)
        status = response.status_code
        if status != 200:
          print("Server Down", status)  
          exit()

        headers = {
            "Host" : f"{self.__host}",
            "User-Agent" : "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:91.0) Gecko/20100101 Firefox/91.0",
            "Accept" : "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
            "Accept-Language" : "en-US,en;q=0.5",
            "Accept-Encoding" : "gzip, deflate",
            "Content-Type" : "application/x-www-form-urlencoded",
            "Origin" : f"https://{self.__host}/",
            "DNT" : "1",    
            "Connection" : "keep-alive",
            "Referer" : f"https://{self.__host}/",
            "Upgrade-Insecure-Requests" : "1",
            "Sec-Fetch-Dest" : "document",
            "Sec-Fetch-Mode" : "navigate",
            "Sec-Fetch-Site" : "same-origin",
            "Sec-Fetch-User" : "?1",
            "TE" : "trailers"
        }

        data = {
            "IpAddress": "",
            "Dni": dni,
            "Usuario": user,
            "Password": password
        }  

        try:
            response = self.__s.post(url = f"https://{self.__host}/Login/Ingresar", headers=headers, data = data, allow_redirects=True)

            response.raise_for_status()

            doc = pq(response.text)
            if not doc('#usuarioLogueado'):
                print("Check login credentials")
                errormsg = doc('.callout-danger')
                if errormsg:
                    raise SessionException(errormsg.text())

                raise SessionException('Session cannot be created.  Check the entered information and try again.')

            print("Connected!")
            self.__is_user_logged_in = True
        except Exception as ex:
            self.__is_user_logged_in = False
            exit()

        self.get_portfolio= Portfolio(host=self.__host,session=self.__s,headers=headers)
        

    # ... (All other methods like get_bluechips, get_galpones, etc., exactly as in original - include them when pasting)

    def get_activity(self, comitente, fecha_desde, fecha_hasta, consolida='0'):
        """
        Fetches activity/transactions for the broker (unified platform).
        
        Args:
            comitente (str): Account number (e.g., '47878').
            fecha_desde (str): Start date in 'dd/mm/yyyy' format (e.g., '01/10/2025').
            fecha_hasta (str): End date in 'dd/mm/yyyy' format (e.g., '01/11/2025').
            consolida (str): '0' or '1' for consolidated view (default '0').
        
        Returns:
            list: List of activity dicts (e.g., [{'Comprobante': 'CCTE', ...}]).
        
        Raises:
            ValueError: On API errors.
            requests.RequestException: On network issues.
        """
        # Lazy import for BeautifulSoup
        try:
            from bs4 import BeautifulSoup
        except ImportError:
            raise ImportError("beautifulsoup4 is required for token parsing. Run 'pip install beautifulsoup4'.")

        if not self.__is_user_logged_in:
            print('You must be logged first')
            exit()

        token = None
        use_selenium = False

        # Try requests first
        activity_headers = {
            "Host": self.__host,
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/141.0.0.0 Safari/537.36",
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8",
            "Accept-Language": "es-AR,es;q=0.9,en;q=0.8",
            "Accept-Encoding": "gzip, deflate, br",
            "DNT": "1",
            "Connection": "keep-alive",
            "Upgrade-Insecure-Requests": "1",
            "Sec-Fetch-Dest": "document",
            "Sec-Fetch-Mode": "navigate",
            "Sec-Fetch-Site": "same-origin",
            "Sec-Fetch-User": "?1",
            "Referer": f"https://{self.__host}/",
        }
        activity_page = self.__s.get(f"https://{self.__host}/Activity", headers=activity_headers)
        activity_page.raise_for_status()
        soup = BeautifulSoup(activity_page.text, 'html.parser')
        token_elem = soup.find('input', {'name': '__RequestVerificationToken'})
        if token_elem:
            token = token_elem.get('value')
            print("DEBUG: Token fetched via requests.")
        else:
            # Fallback to Selenium for JS-loaded token
            try:
                from selenium import webdriver
                from selenium.webdriver.chrome.service import Service
                from selenium.webdriver.common.by import By
                from selenium.webdriver.support.ui import WebDriverWait
                from selenium.webdriver.support import expected_conditions as EC
                from webdriver_manager.chrome import ChromeDriverManager
                print("DEBUG: Token not in requests HTML—using Selenium for JS render.")
                use_selenium = True
                options = webdriver.ChromeOptions()
                options.add_argument('--headless')
                options.add_argument('--no-sandbox')
                options.add_argument('--disable-dev-shm-usage')
                service = Service(ChromeDriverManager().install())
                driver = webdriver.Chrome(service=service, options=options)
                driver.get(f"https://{self.__host}/Activity")
                # Wait for form or token input (up to 10s)
                wait = WebDriverWait(driver, 10)
                token_elem = wait.until(EC.presence_of_element_located((By.NAME, '__RequestVerificationToken')))
                token = token_elem.get_attribute('value')
                driver.quit()
                print("DEBUG: Token fetched via Selenium.")
            except ImportError:
                print("DEBUG: Selenium not installed—falling back to no token (may fail with 403). Install with 'pip install selenium webdriver-manager'.")
            except Exception as se:
                print(f"DEBUG: Selenium failed ({se})—falling back to no token.")

        # Set token if found
        if token:
            self.__s.cookies.set('__RequestVerificationToken', token)

        # Step 2: POST to /Activity/GetActivity (with or without token)
        headers = {
            "Accept": "application/json, text/javascript, */*; q=0.01",
            "Accept-Language": "es-AR,es;q=0.9,en;q=0.8",
            "Content-Type": "application/json; charset=UTF-8",
            "DNT": "1",
            "Origin": f"https://{self.__host}",
            "Priority": "u=1, i",
            "Referer": f"https://{self.__host}/Activity",
            "Sec-Ch-Ua": '"Google Chrome";v="141", "Not?A_Brand";v="8", "Chromium";v="141"',
            "Sec-Ch-Ua-Mobile": "?0",
            "Sec-Ch-Ua-Platform": '"Windows"',
            "Sec-Fetch-Dest": "empty",
            "Sec-Fetch-Mode": "cors",
            "Sec-Fetch-Site": "same-origin",
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/141.0.0.0 Safari/537.36",
            "X-Requested-With": "XMLHttpRequest",
        }

        payload = {
            'consolida': consolida,
            'comitente': str(comitente),
            'fechaDesde': fecha_desde,
            'fechaHasta': fecha_hasta,
        }

        response = self.__s.post(
            f"https://{self.__host}/Activity/GetActivity",
            headers=headers,
            json=payload
        )
        print(f"DEBUG: POST status: {response.status_code}")
        if response.status_code != 200:
            print(f"DEBUG: POST response text: {response.text[:500]}")
        response.raise_for_status()
        data = response.json()
        if not data.get('Success', False):
            error = data.get('Error', {})
            raise ValueError(f"API error: Codigo={error.get('Codigo')}, Descripcion={error.get('Descripcion')}")

        return data.get('Result', [])  # List of activity dicts

    # [Include all other original methods here - get_bluechips, get_galpones, get_cedear, get_bonds, get_short_term_bonds, get_corporate_bonds, account, get_options, get_MERVAL, get_personal_portfolio, get_repos, get_daily_history - copy from previous]

    #########################
    #### PRIVATE METHODS ####
    #########################
    def __convert_datetime_to_epoch(self, dt):

        if isinstance(dt, str):
            dt = datetime.datetime.strptime(dt, '%Y-%m-%d')

        dt_zero = datetime.date(1970, 1, 1)
        time_delta = dt - dt_zero
        return int(time_delta.total_seconds())
    
    def __get_broker_data(self, broker_id):

        broker_data = [broker for broker in brokers if broker['broker_id'] == broker_id]

        if not broker_data:
            supported_brokers = ''.join([str(broker['broker_id']) + ', ' for broker in brokers])[0:-2]
            raise BrokerNotSupportedException('Broker not supported.  Brokers supported: {}.'.format(supported_brokers))

        return broker_data[0]
