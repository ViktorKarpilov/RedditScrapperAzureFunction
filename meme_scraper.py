import time
import urllib
import urllib.request
import re
import urllib.error
from PIL import Image
import psycopg2

conn = psycopg2.connect(
    host="tai.db.elephantsql.com",
    database="tqyeleua",
    user="tqyeleua",
    password="eS6ATv0g9a9NyF4PGnJq9cRqT50_0y_0")

def GetSources():
  cur = conn.cursor()
  cur.execute('SELECT * from sources')
  sources = cur.fetchall()
  cur.close()
  return sources

def SetLastImage(thread_name, channel, last_image):
  cur = conn.cursor()
  cur.execute("""UPDATE public.sources
	SET last_image= %s
	WHERE thread_name=%s AND channel=%s""", (last_image, thread_name, channel))
  conn.commit()
  cur.close()

import telebot
bot = telebot.TeleBot("968562660:AAF25RQVmUlcRYvo_MK9xw06vxhSaYdo0-k")

def PostMemeByUrl(url, channel):
  image = urllib.request.urlopen(url)
  image_parsed = Image.open(image)
  bot.send_photo(chat_id=channel, photo=image_parsed)

def ExtractImageUrlsFromHtml(htmlcontent):
  image_url_regex = "data-url=\"([^<]+?\.jpg)\""
  pattern = re.compile(image_url_regex)
  return re.findall(pattern,htmlcontent)

def ProcessChanngel(thread_name, channel, last_image, htmltext, images_count_limit):

# First imgage processing
  if last_image == "":
    target_url = ExtractImageUrlsFromHtml(htmltext)[-1]
    title = target_url.split('/')[-1]
    PostMemeByUrl(target_url, channel)
    SetLastImage(thread_name, channel, title)
    return

  images_count = 0
  extracted = ExtractImageUrlsFromHtml(htmltext)
  firstTilte = extracted[0].split('/')[-1]

  for image_url in extracted:
    title = image_url.split('/')[-1]
    if(title == last_image or images_count >= int(images_count_limit)):
      break

    images_count += 1

    title = image_url.split('/')[-1]
    PostMemeByUrl(image_url, channel)

  SetLastImage(thread_name, channel, firstTilte)


def GetThreadHtmlFile(thread_name):
  url = "https://old.reddit.com/r/" + thread_name + "/"

  sleep_time = 1

  while sleep_time <= 32:
    try:
      htmlfile = urllib.request.urlopen(url)
      break
    except urllib.error.URLError as e:
      if(e.code != 429 or sleep_time >= 32):
        print(e)
        raise e

      sleep_time *= 2
      print("sleeping for " + str(sleep_time) + " seconds, reason: " + e.reason)
      time.sleep(sleep_time)
      continue

  htmltext = htmlfile.read().decode('utf-8')
  return htmltext

sources = GetSources()
for source in sources:
  thread_name, channel, last_image, images_count_limit = source

  htmltext = GetThreadHtmlFile(thread_name)
  ProcessChanngel(thread_name,
   channel,
   last_image,
   htmltext,
   images_count_limit= images_count_limit)
