import os
from dotenv import load_dotenv
load_dotenv()

PASSKEY = os.getenv('PASSKEY')
HOST_KEY = os.getenv('HOST_KEY')

import sys
from flask import Flask

from bs4 import BeautifulSoup
import re
import requests

import psycopg
from psycopg.rows import dict_row
connection_check_string = ""

from flask import request
import urllib.parse

def execute_select_one(sql: str, passkey: str):
  try:
    conn = psycopg.connect("dbname=neondb user=neondb_owner password=npg_IEzM"+passkey+" host=ep-tiny-dew-a1t"+HOST_KEY+"-pooler.ap-southeast-1.aws.neon.tech port=5432", row_factory=dict_row)
    cur = conn.cursor()

    cur.execute(sql)

    return cur.fetchone()

  except Exception as ex:
    return ex
  finally:
    if cur:
      cur.close()
    if conn:
      conn.close()

def execute_select_all(sql: str, passkey: str):
  try:
    conn = psycopg.connect("dbname=neondb user=neondb_owner password=npg_IEzM"+passkey+" host=ep-tiny-dew-a1t"+HOST_KEY+"-pooler.ap-southeast-1.aws.neon.tech port=5432", row_factory=dict_row)
    cur = conn.cursor()

    cur.execute(sql)
    result = cur.fetchall()

    print(result)
    return result

  except Exception as ex:
    return ex
  finally:
    if cur:
      cur.close()
    if conn:
      conn.close()

def execute_modify(sql: str, passkey: str):
  try:
    conn = psycopg.connect("dbname=neondb user=neondb_owner password=npg_IEzM"+passkey+" host=ep-tiny-dew-a1t"+HOST_KEY+"-pooler.ap-southeast-1.aws.neon.tech port=5432")
    cur = conn.cursor()

    cur.execute(sql)
    conn.commit()
    return {"status": 201}

  except Exception as ex:
    return ex
  finally:
    if cur:
      cur.close()
    if conn:
      conn.close()

def get_url_data(url):
  response = requests.get(url)
  response.encoding = response.apparent_encoding
  html     = response.text
  soup     = BeautifulSoup(html, 'html.parser')

  all_h1 = soup.find_all("h1")
  if not all_h1:
    return None
  soup_title_appl = all_h1[0].text
  for i in range(len(soup_title_appl)):
    if soup_title_appl[i] not in [" ", "\n"]:
      title = soup_title_appl[i:]
      break

  title = title.split("\n")[0]
  soup_text = " ".join(soup.stripped_strings)[:1000]
  description = soup_text

  soup_a = soup.find_all('a')

  hrefs = []
  for link in soup_a:
    hrefs.append(link.get("href"))

  response = {
    "title": title,
    "text": soup_text,
    "hrefs": hrefs
  }

  # check_existing = execute_select_one("select * from url_app_appl")
  # response["existing"] = check_existing

  return response

def register_one_href(href, passkey):
  url = href
  title = ""
  description = ""
  if not execute_select_one(f"select (id, url) from url_app_appl where url = '{url}'", passkey):
    db_response = execute_modify(
      f"insert into url_app_appl(url, title, description) values ('{url}', '{title}', '{description}')", passkey
    )

def register_one_href_suggested(url, href, passkey):
  if not execute_select_one(f"select (id, url_to) from url_app_appl_suggested where url_from = '{url}' and url_to = '{href}'", passkey):
    db_response = execute_modify(
      f"insert into url_app_appl_suggested(url_from, url_to) values ('{url}', '{href}')", passkey
    )

def get_latest_url_app_appl(passkey, interval=5):
  url_data = execute_select_all(f"select url from url_app_appl order by updated_at desc limit {interval}", passkey)
  return url_data

def get_oldest_url_app_appl(passkey, interval=5):
  url_data = execute_select_all(f"select url from url_app_appl order by updated_at asc limit {interval}", passkey)
  for url_dict in url_data:
    url = url_dict["url"]
    execute_modify(f"update url_app_appl set updated_at = CURRENT_TIMESTAMP where url = '{url}'", passkey)
  return url_data

def get_all_top_300_url(passkey):
  url_data = execute_select_all("""
  select url_to, count(url_from) as score
  from url_app_appl
  inner join url_app_appl_suggested on url_app_appl.url = url_app_appl_suggested.url_to
  group by url_to
  order by score desc
  limit 300
  """)
  return url_data

def update_description(url, title, description, passkey):
  url_data = execute_select_one(f"select url from url_app_appl where url = '{url}'", passkey)
  if url_data:
    execute_modify(f"update url_app_appl set title = '{title}', description = '{description}', updated_at = CURRENT_TIMESTAMP where url = '{url}'", passkey)


def update_oldest_url_app_appl(passkey):
  oldest_url_data = get_oldest_url_app_appl(passkey, interval=1)
  
  for url_dict in oldest_url_data:
    url = url_dict["url"]

    if url.startswith("https://apps.apple.com/jp/app/"):
      response = get_url_data(url)
      if not response:
        continue
      update_description(url, urllib.parse.quote(response["title"]), urllib.parse.quote(response["text"]), passkey)
      hrefs = response["hrefs"]

      for href in hrefs:
        if href.startswith("https://apps.apple.com/jp/app/"):
          register_one_href(href, passkey)
          register_one_href_suggested(url, href, passkey)
      
  return {"status": 202, "url": url}

if __name__ == "__main__":
  passkey = PASSKEY
  # passkey = sys.argv[1].strip()
  print(len(passkey) * "*")
  while True:
    result = update_oldest_url_app_appl(passkey)
    print(result)
