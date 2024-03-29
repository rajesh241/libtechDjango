import json
import os
import sys
import csv
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

from nrega.models import State,District,Block,Panchayat,Muster,LibtechTag,CrawlState,Location
from nrega import models  as nregamodels

def argsFetch():
  '''
  Paser for the argument list that returns the args list
  '''
  import argparse

  parser = argparse.ArgumentParser(description='These scripts will initialize the Database for the district and populate relevant details')
  parser.add_argument('-l', '--log-level', help='Log level defining verbosity', required=False)
  parser.add_argument('-n', '--name', help='Name of location that needs to be imported allowed values are {district,block,panchayat}', required=False)
  parser.add_argument('-e', '--export', help='Export Json Data', required=False,action='store_const', const=1)
  parser.add_argument('-mn', '--modelName', help='ModelName that needs to be export or imported', required=False)
  parser.add_argument('-ufn', '--uniqueFieldName', help='ModelName that needs to be export or imported', required=False)
  parser.add_argument('-i', '--import', help='import Json Data', required=False,action='store_const', const=1)
  parser.add_argument('-p', '--populateLocation', help='Populate Locations', required=False,action='store_const', const=1)
  parser.add_argument('-t', '--test', help='A Test Loop', required=False,action='store_const', const=1)

  args = vars(parser.parse_args())
  return args


def main():
  args = argsFetch()
  logger = loggerFetch(args.get('log_level'))
  if args['test']:
    objs=Location.objects.all()
    for obj in objs:
      if obj.locationType == 'state':
        stateCode=obj.code
        filepath="%s/%s/%s/%s/%s" % ("nrega",obj.slug,"DATA","NICREPORTS","filename")
      elif obj.locationType == 'district':
        stateCode=obj.parentLocation.code
        filepath="%s/%s/%s/%s/%s/%s" % ("nrega",obj.parentLocation.slug,obj.slug,"DATA","NICREPORTS","filename")
      elif obj.locationType == 'block':
        stateCode=obj.parentLocation.parentLocation.code
        filepath="%s/%s/%s/%s/%s/%s/%s" % ("nrega",obj.parentLocation.parentLocation.slug,obj.parentLocation.slug,obj.slug,"DATA","NICREPORTS","filename")
      else:
        stateCode=obj.parentLocation.parentLocation.parentLocation.code
        filepath="%s/%s/%s/%s/%s/%s/%s/%s" % ("nrega",obj.parentLocation.parentLocation.parentLocation.slug,obj.parentLocation.parentLocation.slug,obj.parentLocation.slug,obj.slug,"DATA","NICREPORTS","filename")
      obj.stateCode=stateCode
      obj.filepath=filepath
      obj.save()
    exit(0)
    lts=LibtechTag.objects.all()
    for lt in lts:
      logger.info(lt.name)
  if args['import']:

    modelName=args['modelName']
    uniqueFieldName=args['uniqueFieldName']
    if modelName is None:
      logger.info("Nothing to import")
    else:
      fieldArray=[]
      foreignKeyFieldArray=[]
      for f in getattr(nregamodels,modelName)._meta.get_fields(include_parents=False):
          if ( (f.many_to_one or f.one_to_one)): 
            foreignKeyFieldArray.append(f.name)
          elif f.auto_created:
            logger.info("Primary Key")
          else:
            fieldArray.append(f.name)
      jsonName='%s.json' % (modelName.lower())
      json_data=open(jsonName,encoding='utf-8-sig').read()
      d = json.loads(json_data)
      for i,modelDict in d.items():
        uniqueFieldValue=modelDict[uniqueFieldName]
        logger.info(f'{uniqueFieldName} has value {uniqueFieldValue}')
        obj=getattr(nregamodels,modelName).objects.filter(**{ uniqueFieldName: uniqueFieldValue }).first()
        if obj is None:
          obj=getattr(nregamodels,modelName).objects.create(**{ uniqueFieldName: uniqueFieldValue })

        for field,value in modelDict.items():
          if field not in foreignKeyFieldArray:
            setattr(obj,field,value) 
            #logger.info("Field %s Value %s " % (field,value))
          else:
            if value is not None:
              foreignKeyValue=getattr(nregamodels,modelName).objects.filter(**{ uniqueFieldName: value }).first()
              setattr(obj,field,foreignKeyValue)
        logger.info(f'Saving {obj.id} - {obj.name}')    
        obj.save()
    exit(0)

    logger.info("Importing Data in to the system")

    #Start with Populating Crawl States
  #Now we will import Stetes
    jsonName='states.json'
    logger.info("Json file name is %s " % (jsonName))
    json_data=open(jsonName,encoding='utf-8-sig').read()
    d = json.loads(json_data)
    for key,values in d.items():
      logger.info(values)
      obj=State.objects.filter(code=key).first()
      if obj is None:
        obj=State.objects.create(code=key)
      obj.name=values['name']
      obj.crawlIP=values['crawlIP']
      obj.isNIC=values['isNIC']
      obj.englishName=values['englishName']
      obj.stateShortCode=values['stateShortCode']
      obj.save()
    # Now populating Districsts Block and Panchayats
    if args['name']:
      name=args['name']
      jsonName='%ss.json' % (name)
      logger.info("Json file name is %s " % (jsonName))
      json_data=open(jsonName,encoding='utf-8-sig').read()
      d = json.loads(json_data)
      if name=='crawlState':
        for key,values in d.items():
          name=key
          obj=CrawlState.objects.filter(name=name).first()
          if obj is None:
            obj=CrawlState.objects.create(name=name)
          logger.info(name)
          for fieldName,fieldValue in values.items():
            setattr(obj,fieldName,fieldValue)
          obj.save()
        
      if name=='panchayat':
        for key,values in d.items():
          panchayatCode=key
          logger.info(panchayatCode)
          panchayatName=values['name']
          blockCode=values['blockCode']
          logger.info(key)
          eachBlock=Block.objects.filter(code=blockCode).first()
          if eachBlock is not None:
            eachPanchayat=Panchayat.objects.filter(code=panchayatCode).first()
            if eachPanchayat is None:
              Panchayat.objects.create(block=eachBlock,code=panchayatCode,name=panchayatName)
            else:
              eachPanchayat.name=panchayatName
              eachPanchayat.block=eachBlock
              eachPanchayat.englishName=values['englishName']
              eachPanchayat.save()
          else:
            logger.info("Block with code %s does not exists" % blockCode)
      
      if name=='block':
        for key,values in d.items():
          blockCode=key
          blockName=values['name']
          districtCode=values['districtCode']
          eachDistrict=District.objects.filter(code=districtCode).first()
          if eachDistrict is not None:
            eachBlock=Block.objects.filter(code=blockCode).first()
            if eachBlock is None:
              eachBlock=Block.objects.create(code=blockCode,district=eachDistrict,name=blockName)
            else:
              eachBlock.district=eachDistrict
              eachBlock.name=blockName
              eachBlock.englishName=values['englishName']
              eachBlock.save()
          else:
            logger.info("District with code %s does not exist" % (districtCode))
      
      if name=='district':
        for key,values in d.items():
          districtCode=key
          districtName=values['name']
          stateCode=values['stateCode']
          eachState=State.objects.filter(code=stateCode).first()
          if eachState is not None:
            eachDistrict=District.objects.filter(code=districtCode).first()
            if eachDistrict is None:
              eachDistrict=District.objects.create(code=districtCode,state=eachState,name=districtName)
            else:
              eachDistrict.state=eachState
              eachDistrict.name=districtName
              eachDistrict.englishName=values['englishName']
              eachDistrict.save()
          else:
            logger.info("State with code %s does not exist" % (stateCode))
  if args['populateLocation']:
    objs=State.objects.all()
    objs=State.objects.all()[:1]
    for obj in objs:
      try:
        l=Location.objects.create(code=obj.code)
      except:
        l=Location.objects.filter(code=obj.code).first()
      l.name=obj.name
      l.englishName=obj.englishName
      l.nameInLocalLanguage=obj.nameInLocalLanguage
      l.crawlIP=obj.crawlIP
      l.stateShortCode=obj.stateShortCode
      l.isNIC=obj.isNIC
      l.locationType='state'
      l.displayName=obj.name
      l.save()

    objs=District.objects.all()
    objs=District.objects.all()[:1]
    for obj in objs:
      try:
        l=Location.objects.create(code=obj.code)
      except:
        l=Location.objects.filter(code=obj.code).first()
      pl=Location.objects.filter(code=obj.state.code).first()
      l.parentLocation=pl
      l.name=obj.name
      l.englishName=obj.englishName
      l.nameInLocalLanguage=obj.nameInLocalLanguage
      l.crawlIP=obj.state.crawlIP
      l.stateShortCode=obj.state.stateShortCode
      l.isNIC=obj.state.isNIC
      l.locationType='district'
      l.displayName="%s-%s" % (obj.state.name,obj.name)
      l.save()

    objs=Block.objects.all()
    objs=Block.objects.all()[:1]
    for obj in objs:
      try:
        l=Location.objects.create(code=obj.code)
      except:
        l=Location.objects.filter(code=obj.code).first()
      pl=Location.objects.filter(code=obj.district.code).first()
      l.parentLocation=pl
      l.name=obj.name
      l.englishName=obj.englishName
      l.nameInLocalLanguage=obj.nameInLocalLanguage
      l.crawlIP=obj.district.state.crawlIP
      l.stateShortCode=obj.district.state.stateShortCode
      l.isNIC=obj.district.state.isNIC
      l.locationType='block'
      l.displayName="%s-%s-%s" % (obj.district.state.name,obj.district.name,obj.name)
      l.save()
 
    objs=Panchayat.objects.all()
    for obj in objs:
      try:
        l=Location.objects.create(code=obj.code)
      except:
        l=Location.objects.filter(code=obj.code).first()
      pl=Location.objects.filter(code=obj.block.code).first()
      l.parentLocation=pl
      l.name=obj.name
      l.englishName=obj.englishName
      l.nameInLocalLanguage=obj.nameInLocalLanguage
      l.crawlIP=obj.block.district.state.crawlIP
      l.stateShortCode=obj.block.district.state.stateShortCode
      l.isNIC=obj.block.district.state.isNIC
      l.locationType='panchayat'
      l.displayName="%s-%s-%s-%s" % (obj.block.district.state.name,obj.block.district.name,obj.block.name,obj.name)
      l.save()
     
  if args['export']:
    modelName=args['modelName']
    if modelName is None:
      logger.info("Nothing to Export")
    else:
      logger.info("Going to export %s JSON Data" % (modelName))
      fieldArray=[]
      foreignKeyFieldArray=[]
      for f in getattr(nregamodels,modelName)._meta.get_fields(include_parents=False):
          if ( (f.many_to_one or f.one_to_one)): 
            foreignKeyFieldArray.append(f.name)
          elif f.auto_created:
            logger.info("Primary Key")
          else:
            fieldArray.append(f.name)
      d={}
      i=0
      objs=getattr(nregamodels,modelName).objects.all()
      for obj in objs:
        logger.info(obj.id)
        p={}
        for field in fieldArray:
          p[field]=getattr(obj,field)
        for field in foreignKeyFieldArray:
          try:
            p[field]=str(getattr(obj,field))
          except:
            p[field]=None
        i=i+1
        d[i]=p
      with open('%s.json' % (modelName.lower()), 'w') as f:
        json.dump(d, f, ensure_ascii=False)
      
    exit(0)
    logger.info("Going to export location JSON Data")
    allStates=State.objects.all()
    d=dict()
    for eachState in allStates:
      logger.info(eachState.code)
      p=dict()
      p['stateCode']=eachState.code
      p['name']=eachState.name
      p['englishName']=eachState.englishName
      p['crawlIP']=eachState.crawlIP
      p['isNIC']=eachState.isNIC
      p['stateShortCode']=eachState.stateShortCode
      d[eachState.code]=p
    with open('states.json', 'w') as f:
      json.dump(d, f, ensure_ascii=False)
      
    crawlStates=CrawlState.objects.all()
    d=dict()
    for cs in crawlStates:
      p=dict()
      p['name']=cs.name
      p['sequence']=cs.sequence
      p['minhour']=cs.minhour
      p['maxhour']=cs.maxhour
      p['isBlockLevel']=cs.isBlockLevel
      p['isDistrictLevel']=cs.isDistrictLevel
      p['needFullBlockData']=cs.needFullBlockData
      p['iterateFinYear']=cs.iterateFinYear
      d[cs.name]=p
    with open('crawlStates.json', 'w') as f:
      json.dump(d, f, ensure_ascii=False)
    exit(0)
    allDistricts=District.objects.all()
    d=dict()
    for eachDistrict in allDistricts:
      logger.info(eachDistrict.code)
      p=dict()
      p['stateCode']=eachDistrict.state.code
      p['name']=eachDistrict.name
      p['englishName']=eachDistrict.englishName
      d[eachDistrict.code]=p
    with open('/tmp/districts.json', 'w') as f:
      json.dump(d, f, ensure_ascii=False)
    allBlocks=Block.objects.all()
    d=dict()
    for eachBlock in allBlocks:
      logger.info(eachBlock.code)
      p=dict()
      p['districtCode']=eachBlock.district.code
      p['name']=eachBlock.name
      p['englishName']=eachBlock.englishName
      d[eachBlock.code]=p
    with open('/tmp/blocks.json', 'w') as f:
      json.dump(d, f, ensure_ascii=False)

    allPanchayats=Panchayat.objects.all()
    d=dict()
    for eachPanchayat in allPanchayats:
      p=dict()
      p['blockCode']=eachPanchayat.block.code
      p['name']=eachPanchayat.name
      p['englishName']=eachPanchayat.englishName
      d[eachPanchayat.code]=p
    with open('/tmp/panchayats.json', 'w') as f:
      json.dump(d, f, ensure_ascii=False)


  logger.info("...END PROCESSING") 
  exit(0)

if __name__ == '__main__':
  main()
