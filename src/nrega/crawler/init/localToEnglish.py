import json
from google.cloud import translate
import os
import sys
fileDir = os.path.dirname(os.path.realpath(__file__))
rootDir=fileDir+"/../../../"
sys.path.insert(0, rootDir)
from config.defines import djangoSettings
from nrega.crawler.commons.nregaFunctions import loggerFetch
import django
from django.core.wsgi import get_wsgi_application
from django.core.files.base import ContentFile
from django.utils import timezone
from django.db.models import F,Q,Sum,Count
os.environ.setdefault("DJANGO_SETTINGS_MODULE", djangoSettings)
django.setup()

from nrega.models import State,District,Block,Panchayat,Muster,LibtechTag,CrawlState
from nrega import models  as nregamodels

def argsFetch():
  '''
  Paser for the argument list that returns the args list
  '''
  import argparse

  parser = argparse.ArgumentParser(description='These scripts will initialize the Database for the district and populate relevant details')
  parser.add_argument('-l', '--log-level', help='Log level defining verbosity', required=False)
  parser.add_argument('-n', '--name', help='Name of location that needs to be imported allowed values are {district,block,panchayat}', required=False)
  parser.add_argument('-mn', '--modelName', help='Name of Models whose name has to be translated', required=False)
  parser.add_argument('-e', '--export', help='Export Json Data', required=False,action='store_const', const=1)
  parser.add_argument('-i', '--import', help='import Json Data', required=False,action='store_const', const=1)
  parser.add_argument('-t1', '--test', help='A Test Loop', required=False,action='store_const', const=1)
  parser.add_argument('-t', '--translate', help='A Test Loop', required=False,action='store_const', const=1)

  args = vars(parser.parse_args())
  return args


def isEnglish(s):
    try:
        s.encode(encoding='utf-8').decode('ascii')
    except UnicodeDecodeError:
        return False
    else:
        return True

def main():
  args = argsFetch()
  logger = loggerFetch(args.get('log_level'))
  if args['translate']:
    translate_client = translate.Client()
    target="en"
    modelName=args['modelName']
    objs=getattr(nregamodels,modelName).objects.all()
    for obj in objs:
      a=isEnglish(obj.name)
      if a is False:
        translation = translate_client.translate(obj.name,target_language=target)
        logger.info(translation)
        englishName=translation['translatedText']
        nameInLocalLanguage=True
      else:
        englishName=obj.name
        nameInLocalLanguage=False
      logger.info(f'{obj.name} - {englishName}')
      obj.englishName=englishName
      obj.nameInLocalLanguage=nameInLocalLanguage
      obj.save()
  if args['test']:
    i=1
    myLocations=Panchayat.objects.all()
    for eachLocation in myLocations:
      a=isEnglish(eachLocation.name)
      if a is False:
        logger.info(f" {i} - {eachLocation.name}")
        i=i+1
    exit(0)
    name=args['name']
    translate_client = translate.Client()
    target="en"
    translation = translate_client.translate(name,target_language=target)
    logger.info(translation)
    exit(0)
    logger.info("Running test")
    myBlocks=Block.objects.all()
    for eachBlock in myBlocks:
      a=isEnglish(eachBlock.name)
      if a is False:
        logger.info(f" {i} - {eachBlock.name}")
        i=i+1
  logger.info("...END PROCESSING") 
  exit(0)

if __name__ == '__main__':
  main()
