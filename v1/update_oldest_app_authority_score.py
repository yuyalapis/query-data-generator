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
import datetime
import time

def execute_select_one(sql: str):
  try:
    conn = psycopg.connect("dbname=neondb user=neondb_owner password=npg_IEzM"+PASSKEY+" host=ep-tiny-dew-a1t"+HOST_KEY+"-pooler.ap-southeast-1.aws.neon.tech port=5432", row_factory=dict_row)
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

def execute_select_all(sql: str):
  try:
    conn = psycopg.connect("dbname=neondb user=neondb_owner password=npg_IEzM"+PASSKEY+" host=ep-tiny-dew-a1t"+HOST_KEY+"-pooler.ap-southeast-1.aws.neon.tech port=5432", row_factory=dict_row)
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

def execute_modify(sql: str):
  try:
    conn = psycopg.connect("dbname=neondb user=neondb_owner password=npg_IEzM"+PASSKEY+" host=ep-tiny-dew-a1t"+HOST_KEY+"-pooler.ap-southeast-1.aws.neon.tech port=5432")
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

def save_oldest_app_authority_score():
  url_dict = execute_select_one("select url from url_app_appl order by app_authority_score_saved_at asc limit 1")
  url = url_dict["url"]
  current_date = (datetime.datetime.now() + datetime.timedelta(hours=9)).date()
  print(current_date)
  app_score_log = execute_select_one(f"select * from app_authority_score_logs where saved_at = '{current_date}' and url = '{url}'")
  # print(app_score_log)
  if not app_score_log:
    print("generating app_authority_score_logs")
    top_app_score_dict = execute_select_one("select url_to, count(url_from) as score from url_app_appl_suggested group by url_to order by score desc limit 1")
    # print(top_app_score_dict)
    target_app_score_dict = execute_select_one(f"select url_to, count(url_from) as score from url_app_appl_suggested where url_to = '{url}' group by url_to")
    # print(target_app_score_dict)
    target_app_score = target_app_score_dict["score"] / top_app_score_dict["score"]
    print(target_app_score)
    modified_dict = execute_modify(f"insert into app_authority_score_logs(saved_at, url, app_authority_score) values ('{current_date}', '{url}', '{target_app_score}')")
    # print(modified_dict)
    updated_dict = execute_modify(f"update url_app_appl set app_authority_score_saved_at = '{current_date}' where url = '{url}'")
    print(updated_dict)

  return {"status": 201, "url": url}

if __name__ == "__main__":
  print(len(PASSKEY) * "*")
  while True:
    try:
      result = save_oldest_app_authority_score()
      print(result)
      time.sleep(1)
except Exception as e:
      print("app authority score exception", e)
